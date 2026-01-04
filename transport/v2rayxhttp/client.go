package v2rayxhttp

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	sHTTP "github.com/sagernet/sing/protocol/http"

	"golang.org/x/net/http2"
)

var _ adapter.V2RayClientTransport = (*Client)(nil)

type Client struct {
	ctx                  context.Context
	dialer               N.Dialer
	serverAddr           M.Socksaddr
	transport            http.RoundTripper
	requestURL           url.URL
	host                 []string
	headers              http.Header
	mode                 string
	maxUploadSize        int32
	maxConcurrentUploads int32
	minUploadInterval    time.Duration
	paddingMin           int32
	paddingMax           int32
	http2                bool
}

func NewClient(ctx context.Context, dialer N.Dialer, serverAddr M.Socksaddr, options option.V2RayXHTTPOptions, tlsConfig tls.Config) (adapter.V2RayClientTransport, error) {
	var transport http.RoundTripper
	var http2Enabled bool

	if tlsConfig == nil {
		transport = &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return dialer.DialContext(ctx, network, M.ParseSocksaddr(addr))
			},
			DisableKeepAlives: true,
		}
	} else {
		if len(tlsConfig.NextProtos()) == 0 {
			tlsConfig.SetNextProtos([]string{http2.NextProtoTLS, "http/1.1"})
		}
		transport = &http2.Transport{
			DialTLSContext: func(ctx context.Context, network, addr string, cfg *tls.STDConfig) (net.Conn, error) {
				conn, err := dialer.DialContext(ctx, network, M.ParseSocksaddr(addr))
				if err != nil {
					return nil, err
				}
				return tls.ClientHandshake(ctx, conn, tlsConfig)
			},
		}
		http2Enabled = true
	}

	var requestURL url.URL
	if tlsConfig == nil {
		requestURL.Scheme = "http"
	} else {
		requestURL.Scheme = "https"
	}
	requestURL.Host = serverAddr.String()
	requestURL.Path = options.Path
	err := sHTTP.URLSetPath(&requestURL, options.Path)
	if err != nil {
		return nil, E.Cause(err, "parse path")
	}
	if !strings.HasPrefix(requestURL.Path, "/") {
		requestURL.Path = "/" + requestURL.Path
	}
	if !strings.HasSuffix(requestURL.Path, "/") {
		requestURL.Path = requestURL.Path + "/"
	}

	mode := options.Mode
	if mode == "" {
		mode = "auto"
	}

	maxUploadSize := options.MaxUploadSize
	if maxUploadSize <= 0 {
		maxUploadSize = 1000000 // 1MB default
	}

	maxConcurrentUploads := options.MaxConcurrentUploads
	if maxConcurrentUploads <= 0 {
		maxConcurrentUploads = 100
	}

	minUploadInterval := time.Duration(options.MinUploadInterval)
	if minUploadInterval <= 0 {
		minUploadInterval = 30 * time.Millisecond
	}

	var paddingMin, paddingMax int32
	if options.Padding != nil {
		paddingMin = options.Padding.From
		paddingMax = options.Padding.To
	} else {
		paddingMin = 100
		paddingMax = 1000
	}

	return &Client{
		ctx:                  ctx,
		dialer:               dialer,
		serverAddr:           serverAddr,
		requestURL:           requestURL,
		host:                 options.Host,
		headers:              options.Headers.Build(),
		transport:            transport,
		http2:                http2Enabled,
		mode:                 mode,
		maxUploadSize:        maxUploadSize,
		maxConcurrentUploads: maxConcurrentUploads,
		minUploadInterval:    minUploadInterval,
		paddingMin:           paddingMin,
		paddingMax:           paddingMax,
	}, nil
}

func (c *Client) DialContext(ctx context.Context) (net.Conn, error) {
	// Generate session ID (UUID v4)
	sessionId := generateUUID()

	conn := &xhttpClientConn{
		ctx:               ctx,
		client:            c,
		sessionId:         sessionId,
		uploadQueue:       make(chan []byte, 256),
		downloadReader:    nil,
		maxUploadSize:     c.maxUploadSize,
		minUploadInterval: c.minUploadInterval,
	}

	// Start download connection (GET request)
	err := conn.startDownload()
	if err != nil {
		return nil, E.Cause(err, "start download")
	}

	// Start upload goroutine
	go conn.uploadLoop()

	return conn, nil
}

func (c *Client) Close() error {
	if closer, ok := c.transport.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// xhttpClientConn implements net.Conn for the client side
type xhttpClientConn struct {
	ctx               context.Context
	client            *Client
	sessionId         string
	uploadQueue       chan []byte
	downloadReader    io.ReadCloser
	downloadResponse  *http.Response
	maxUploadSize     int32
	minUploadInterval time.Duration
	seq               int64
	closed            bool
	mu                sync.Mutex
}

func (c *xhttpClientConn) startDownload() error {
	downloadURL := c.client.requestURL
	downloadURL.Path = downloadURL.Path + c.sessionId

	request, err := http.NewRequestWithContext(c.ctx, http.MethodGet, downloadURL.String(), nil)
	if err != nil {
		return err
	}

	// Set host
	if len(c.client.host) > 0 {
		request.Host = c.client.host[0]
	}

	// Copy headers
	for key, values := range c.client.headers {
		for _, value := range values {
			request.Header.Add(key, value)
		}
	}

	// Add padding header
	padding := generatePadding(c.client.paddingMin, c.client.paddingMax)
	if len(padding) > 0 {
		request.Header.Set("X-Padding", string(padding))
	}

	response, err := c.client.transport.RoundTrip(request)
	if err != nil {
		return err
	}

	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		return E.New("xhttp: unexpected status: ", response.Status)
	}

	c.downloadReader = response.Body
	c.downloadResponse = response
	return nil
}

func (c *xhttpClientConn) uploadLoop() {
	defer func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
	}()

	var buffer bytes.Buffer
	ticker := time.NewTicker(c.minUploadInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			// Flush remaining data
			if buffer.Len() > 0 {
				c.sendUpload(buffer.Bytes())
			}
			return
		case data, ok := <-c.uploadQueue:
			if !ok {
				// Channel closed, flush remaining data
				if buffer.Len() > 0 {
					c.sendUpload(buffer.Bytes())
				}
				return
			}
			buffer.Write(data)
			// Check if we should send immediately (buffer is large enough)
			if int32(buffer.Len()) >= c.maxUploadSize {
				c.sendUpload(buffer.Bytes())
				buffer.Reset()
			}
		case <-ticker.C:
			// Send accumulated data
			if buffer.Len() > 0 {
				c.sendUpload(buffer.Bytes())
				buffer.Reset()
			}
		}
	}
}

func (c *xhttpClientConn) sendUpload(data []byte) error {
	uploadURL := c.client.requestURL
	uploadURL.Path = fmt.Sprintf("%s%s/%d", uploadURL.Path, c.sessionId, c.seq)
	c.seq++

	request, err := http.NewRequestWithContext(c.ctx, http.MethodPost, uploadURL.String(), bytes.NewReader(data))
	if err != nil {
		return err
	}

	// Set host
	if len(c.client.host) > 0 {
		request.Host = c.client.host[0]
	}

	// Copy headers
	for key, values := range c.client.headers {
		for _, value := range values {
			request.Header.Add(key, value)
		}
	}

	request.ContentLength = int64(len(data))

	// Add padding header
	padding := generatePadding(c.client.paddingMin, c.client.paddingMax)
	if len(padding) > 0 {
		request.Header.Set("X-Padding", string(padding))
	}

	response, err := c.client.transport.RoundTrip(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return E.New("xhttp upload: unexpected status: ", response.Status)
	}

	return nil
}

func (c *xhttpClientConn) Read(p []byte) (n int, err error) {
	if c.downloadReader == nil {
		return 0, io.EOF
	}
	return c.downloadReader.Read(p)
}

func (c *xhttpClientConn) Write(p []byte) (n int, err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	c.mu.Unlock()

	// Make a copy of the data
	data := make([]byte, len(p))
	copy(data, p)

	select {
	case c.uploadQueue <- data:
		return len(p), nil
	case <-c.ctx.Done():
		return 0, c.ctx.Err()
	}
}

func (c *xhttpClientConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()

	close(c.uploadQueue)
	if c.downloadReader != nil {
		c.downloadReader.Close()
	}
	return nil
}

func (c *xhttpClientConn) LocalAddr() net.Addr {
	return &net.TCPAddr{}
}

func (c *xhttpClientConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{
		IP:   net.ParseIP(c.client.serverAddr.AddrString()),
		Port: int(c.client.serverAddr.Port),
	}
}

func (c *xhttpClientConn) SetDeadline(t time.Time) error {
	return nil
}

func (c *xhttpClientConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *xhttpClientConn) SetWriteDeadline(t time.Time) error {
	return nil
}

// generateUUID generates a random UUID v4
func generateUUID() string {
	uuid := make([]byte, 16)
	rand.Read(uuid)
	// Set version (4) and variant (RFC4122)
	uuid[6] = (uuid[6] & 0x0f) | 0x40
	uuid[8] = (uuid[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])
}

// Ensure interface compliance
var _ net.Conn = (*xhttpClientConn)(nil)
