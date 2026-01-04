package v2rayxhttp

import (
	"context"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tls"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/transport/v2rayhttp"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/buf"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	aTLS "github.com/sagernet/sing/common/tls"
	sHttp "github.com/sagernet/sing/protocol/http"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

var _ adapter.V2RayServerTransport = (*Server)(nil)

// sessionIdRegex matches UUID format session IDs
var sessionIdRegex = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type Server struct {
	ctx        context.Context
	logger     logger.ContextLogger
	tlsConfig  tls.ServerConfig
	handler    adapter.V2RayServerTransportHandler
	httpServer *http.Server
	h2Server   *http2.Server
	h2cHandler http.Handler
	host       []string
	path       string
	headers    http.Header
	sessions   sync.Map // map[sessionId]*xhttpSession
	mode       string
}

type xhttpSession struct {
	uploadQueue chan *buf.Buffer
	downReader  *io.PipeReader
	downWriter  *io.PipeWriter
	created     time.Time
	connected   bool
	mu          sync.Mutex
}

func newXHTTPSession() *xhttpSession {
	downReader, downWriter := io.Pipe()
	return &xhttpSession{
		uploadQueue: make(chan *buf.Buffer, 256),
		downReader:  downReader,
		downWriter:  downWriter,
		created:     time.Now(),
	}
}

func (s *xhttpSession) close() {
	close(s.uploadQueue)
	s.downWriter.Close()
	s.downReader.Close()
}

func NewServer(ctx context.Context, logger logger.ContextLogger, options option.V2RayXHTTPOptions, tlsConfig tls.ServerConfig, handler adapter.V2RayServerTransportHandler) (*Server, error) {
	server := &Server{
		ctx:       ctx,
		tlsConfig: tlsConfig,
		logger:    logger,
		handler:   handler,
		h2Server:  &http2.Server{},
		host:      options.Host,
		path:      options.Path,
		headers:   options.Headers.Build(),
		mode:      options.Mode,
	}
	if server.mode == "" {
		server.mode = "auto"
	}
	if !strings.HasPrefix(server.path, "/") {
		server.path = "/" + server.path
	}
	if !strings.HasSuffix(server.path, "/") {
		server.path = server.path + "/"
	}
	server.httpServer = &http.Server{
		Handler:           server,
		ReadHeaderTimeout: C.TCPTimeout,
		MaxHeaderBytes:    http.DefaultMaxHeaderBytes,
		BaseContext: func(net.Listener) context.Context {
			return ctx
		},
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			return log.ContextWithNewID(ctx)
		},
	}
	server.h2cHandler = h2c.NewHandler(server, server.h2Server)

	// Start session cleanup goroutine
	go server.cleanupSessions(ctx)

	return server, nil
}

func (s *Server) cleanupSessions(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			s.sessions.Range(func(key, value any) bool {
				session := value.(*xhttpSession)
				session.mu.Lock()
				// Remove sessions that are older than 30 seconds and not connected
				if !session.connected && now.Sub(session.created) > 30*time.Second {
					session.mu.Unlock()
					s.sessions.Delete(key)
					session.close()
				} else {
					session.mu.Unlock()
				}
				return true
			})
		}
	}
}

func (s *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	// Handle h2c upgrade
	if request.Method == "PRI" && len(request.Header) == 0 && request.URL.Path == "*" && request.Proto == "HTTP/2.0" {
		s.h2cHandler.ServeHTTP(writer, request)
		return
	}

	// Validate host
	host := request.Host
	if len(s.host) > 0 && !common.Contains(s.host, host) {
		s.invalidRequest(writer, request, http.StatusBadRequest, E.New("bad host: ", host))
		return
	}

	// Check if the path matches our prefix
	if !strings.HasPrefix(request.URL.Path, s.path) {
		s.invalidRequest(writer, request, http.StatusNotFound, E.New("bad path: ", request.URL.Path))
		return
	}

	// Extract session ID from path: /{path}/{sessionId} or /{path}/{sessionId}/{seq}
	pathSuffix := strings.TrimPrefix(request.URL.Path, s.path)
	parts := strings.Split(pathSuffix, "/")
	if len(parts) == 0 || parts[0] == "" {
		s.invalidRequest(writer, request, http.StatusBadRequest, E.New("missing session id"))
		return
	}

	sessionId := parts[0]
	if !sessionIdRegex.MatchString(sessionId) {
		s.invalidRequest(writer, request, http.StatusBadRequest, E.New("invalid session id format"))
		return
	}

	// Set common response headers
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Accel-Buffering", "no")
	for key, values := range s.headers {
		for _, value := range values {
			writer.Header().Set(key, value)
		}
	}

	switch request.Method {
	case http.MethodGet:
		s.handleDownload(writer, request, sessionId)
	case http.MethodPost:
		s.handleUpload(writer, request, sessionId)
	default:
		s.invalidRequest(writer, request, http.StatusMethodNotAllowed, E.New("unsupported method: ", request.Method))
	}
}

func (s *Server) handleDownload(writer http.ResponseWriter, request *http.Request, sessionId string) {
	// Get or create session
	sessionRaw, loaded := s.sessions.LoadOrStore(sessionId, newXHTTPSession())
	session := sessionRaw.(*xhttpSession)

	session.mu.Lock()
	if session.connected {
		session.mu.Unlock()
		s.invalidRequest(writer, request, http.StatusConflict, E.New("session already connected"))
		return
	}
	session.connected = true
	session.mu.Unlock()

	// Set SSE headers for better middlebox compatibility
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.WriteHeader(http.StatusOK)
	if flusher, ok := writer.(http.Flusher); ok {
		flusher.Flush()
	}

	source := sHttp.SourceAddress(request)

	// Create connection wrapper
	conn := &xhttpServerConn{
		session:   session,
		writer:    writer,
		request:   request,
		sessionId: sessionId,
		flusher:   writer.(http.Flusher),
		logger:    s.logger,
		loaded:    loaded,
	}

	// If this is a new session (not loaded), wait for upload to establish
	if !loaded {
		go func() {
			// Wait a bit for the upload connection
			time.Sleep(100 * time.Millisecond)
		}()
	}

	done := make(chan struct{})
	s.handler.NewConnectionEx(v2rayhttp.DupContext(request.Context()), conn, source, M.Socksaddr{}, N.OnceClose(func(it error) {
		close(done)
	}))
	<-done

	// Cleanup session
	s.sessions.Delete(sessionId)
	session.close()
}

func (s *Server) handleUpload(writer http.ResponseWriter, request *http.Request, sessionId string) {
	// Get or create session
	sessionRaw, _ := s.sessions.LoadOrStore(sessionId, newXHTTPSession())
	session := sessionRaw.(*xhttpSession)

	// Read the request body
	data, err := io.ReadAll(request.Body)
	if err != nil {
		s.invalidRequest(writer, request, http.StatusBadRequest, E.Cause(err, "read request body"))
		return
	}

	if len(data) > 0 {
		buffer := buf.NewSize(len(data))
		buffer.Write(data)
		select {
		case session.uploadQueue <- buffer:
		default:
			buffer.Release()
			s.invalidRequest(writer, request, http.StatusServiceUnavailable, E.New("upload queue full"))
			return
		}
	}

	writer.WriteHeader(http.StatusOK)
}

func (s *Server) invalidRequest(writer http.ResponseWriter, request *http.Request, statusCode int, err error) {
	if statusCode > 0 {
		writer.WriteHeader(statusCode)
	}
	s.logger.ErrorContext(request.Context(), E.Cause(err, "process connection from ", request.RemoteAddr))
}

func (s *Server) Network() []string {
	return []string{N.NetworkTCP}
}

func (s *Server) Serve(listener net.Listener) error {
	if s.tlsConfig != nil {
		if len(s.tlsConfig.NextProtos()) == 0 {
			s.tlsConfig.SetNextProtos([]string{http2.NextProtoTLS, "http/1.1"})
		} else if !common.Contains(s.tlsConfig.NextProtos(), http2.NextProtoTLS) {
			s.tlsConfig.SetNextProtos(append([]string{http2.NextProtoTLS}, s.tlsConfig.NextProtos()...))
		}
		listener = aTLS.NewListener(listener, s.tlsConfig)
	}
	return s.httpServer.Serve(listener)
}

func (s *Server) ServePacket(listener net.PacketConn) error {
	return os.ErrInvalid
}

func (s *Server) Close() error {
	// Close all sessions
	s.sessions.Range(func(key, value any) bool {
		session := value.(*xhttpSession)
		session.close()
		s.sessions.Delete(key)
		return true
	})
	return common.Close(common.PtrOrNil(s.httpServer))
}

// xhttpServerConn implements net.Conn for the server side
type xhttpServerConn struct {
	session   *xhttpSession
	writer    http.ResponseWriter
	request   *http.Request
	sessionId string
	flusher   http.Flusher
	logger    logger.ContextLogger
	loaded    bool

	readBuffer  *buf.Buffer
	localAddr   net.Addr
	remoteAddr  net.Addr
	readClosed  bool
	writeClosed bool
	mu          sync.Mutex
}

func (c *xhttpServerConn) Read(p []byte) (n int, err error) {
	c.mu.Lock()
	if c.readClosed {
		c.mu.Unlock()
		return 0, io.EOF
	}
	c.mu.Unlock()

	// If we have leftover data from previous read
	if c.readBuffer != nil && !c.readBuffer.IsEmpty() {
		n, _ = c.readBuffer.Read(p)
		if c.readBuffer.IsEmpty() {
			c.readBuffer.Release()
			c.readBuffer = nil
		}
		return n, nil
	}

	// Read from upload queue
	select {
	case buffer, ok := <-c.session.uploadQueue:
		if !ok {
			return 0, io.EOF
		}
		n, _ = buffer.Read(p)
		if !buffer.IsEmpty() {
			c.readBuffer = buffer
		} else {
			buffer.Release()
		}
		return n, nil
	case <-c.request.Context().Done():
		return 0, io.EOF
	}
}

func (c *xhttpServerConn) Write(p []byte) (n int, err error) {
	c.mu.Lock()
	if c.writeClosed {
		c.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	c.mu.Unlock()

	n, err = c.writer.Write(p)
	if err != nil {
		return n, err
	}
	c.flusher.Flush()
	return n, nil
}

func (c *xhttpServerConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readClosed = true
	c.writeClosed = true
	if c.readBuffer != nil {
		c.readBuffer.Release()
		c.readBuffer = nil
	}
	return nil
}

func (c *xhttpServerConn) LocalAddr() net.Addr {
	if c.localAddr != nil {
		return c.localAddr
	}
	return &net.TCPAddr{}
}

func (c *xhttpServerConn) RemoteAddr() net.Addr {
	if c.remoteAddr != nil {
		return c.remoteAddr
	}
	if c.request.RemoteAddr != "" {
		addr, err := net.ResolveTCPAddr("tcp", c.request.RemoteAddr)
		if err == nil {
			c.remoteAddr = addr
			return addr
		}
	}
	return &net.TCPAddr{}
}

func (c *xhttpServerConn) SetDeadline(t time.Time) error {
	return nil
}

func (c *xhttpServerConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *xhttpServerConn) SetWriteDeadline(t time.Time) error {
	return nil
}

// generatePadding generates random padding bytes
func generatePadding(minBytes, maxBytes int32) []byte {
	if minBytes <= 0 && maxBytes <= 0 {
		return nil
	}
	if minBytes < 0 {
		minBytes = 0
	}
	if maxBytes <= minBytes {
		maxBytes = minBytes + 1
	}
	size := minBytes + rand.Int31n(maxBytes-minBytes)
	if size <= 0 {
		return nil
	}
	padding := make([]byte, size)
	for i := range padding {
		padding[i] = byte('a' + rand.Intn(26))
	}
	return padding
}
