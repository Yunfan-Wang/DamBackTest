// support for generic remote interaction over sockets
// including a socket wrapper that can drop and/or delay messages arbitrarily
// works with any* objects that can be gob-encoded for serialization
//
// the LeakySocket wrapper for net.Conn is provided in its entirety, and should
// not be changed, though you may extend it with additional helper functions as
// desired.  it is used directly by the test code.
//
// the RemoteError type is also provided in its entirety, and should not
// be changed.
//
// suggested RequestMsg and ReplyMsg types are included to get you started,
// but they are only used internally to the remote library, so you can use
// something else if you prefer
//
// the CalleeStub type represents the callee that manages remote objects, invokes
// calls from callers, and returns suitable results and/or remote errors
//
// the CallerStubCreator converts a struct of function declarations into a functional
// caller stub by using reflection to populate the function definitions.
//
// USAGE:
// the desired usage of this library is as follows (not showing all error-checking
// for clarity and brevity):
//
//  example ServiceInterface known to both caller and callee, defined as
//  type ServiceInterface struct {
//      ExampleMethod func(int, int) (int, remote.RemoteError)
//  }
//
//  1. callee program calls NewCalleeStub with interface and connection details, e.g.,
//     inst := &ServiceInstance{}
//     callee, err := remote.NewCalleeStub(&ServiceInterface{}, inst, "147.36.147.36:9999", true, true)
//
//  2. caller program calls CallerStubCreator, e.g.,
//     caller := &ServiceInterface{}
//     err := remote.CallerStubCreator(caller, "147.36.147.36:9999", true, true)
//
//  3. caller makes calls, e.g.,
//     n, r := caller.ExampleMethod(42, 14736)
//
//
// TODO *** here's what needs to be done for Lab 1:
//  1. create the CalleeStub type and supporting functions, including but not
//     limited to: NewCalleeStub, Start, Stop, IsRunning, and GetCallCount (see below)
//
//  2. create the CallerStubCreator which uses reflection to transparently define each
//     method call in the caller stub (see below)
//

package remote

import (
	"errors"
	"io"
	"math/rand"
	"net"
	"reflect"
	"time"
	"bytes"
    "encoding/gob"
	"sync"
	"fmt"
	"encoding/binary"
)

// LeakySocket

// LeakySocket is a wrapper for a net.Conn connection that emulates
// transmission delays and random packet loss. it has its own send
// and receive functions that together mimic an unreliable connection
// that can be customized to stress-test remote service interactions.
type LeakySocket struct {
    s           net.Conn
    isLossy     bool
    lossRate    float32
    isDelayed   bool
    msTimeout   int
    usTimeout   int
    msDelay     int
    usDelay     int
}



func isValidRemoteInterface(iface reflect.Type) bool {
	if iface.Kind() != reflect.Struct {
		return false
	}
	for i := 0; i < iface.NumField(); i++ {
		field := iface.Field(i)
		if field.Type.Kind() != reflect.Func {
			continue
		}

		if field.Type.NumOut() < 1 {
			return false
		}

		lastRet := field.Type.Out(field.Type.NumOut() - 1)
		if lastRet != reflect.TypeOf((*RemoteError)(nil)).Elem() {
			return false
		}
	}
	return true
}

// builder for a LeakySocket given a normal socket and indicators
// of whether the connection should experience loss and delay.
// uses default loss and delay values that can be changed using setters.    
func NewLeakySocket(conn net.Conn, lossy bool, delayed bool) *LeakySocket {
    return &LeakySocket{
	    s: conn,
	    isLossy: lossy,
	    lossRate: 0.05,
	    isDelayed: delayed,
	    msTimeout: 500,
	    usTimeout: 0,
	    msDelay: 2,
	    usDelay: 0,
    }
}

// send a byte-string over the socket mimicking unreliability.
// delay is emulated using time.Sleep, packet loss is emulated using RNG
// coupled with time.Sleep to emulate a timeout
//
// Failure scenarios when designing: 
// 1. The isLossy flag is true and a random roll is less than the lossRate meaning
// packet loss, omission failure. 
// 2. The underlying TCP connection ls.s is closed by the peer or lost due to a physical network break.
// Connection Termination (Crash Failure)
func (ls *LeakySocket) Send(obj []byte) (bool, error) {
    if obj == nil {
        return true, nil
    }
    fullMsg := make([]byte, 4+len(obj))
    binary.BigEndian.PutUint32(fullMsg[:4], uint32(len(obj)))
    copy(fullMsg[4:], obj)

    if ls.s != nil {
        rand.Seed(time.Now().UnixNano())
        if ls.isLossy && rand.Float32() < ls.lossRate {
            time.Sleep(time.Duration(ls.msTimeout) * time.Millisecond)
            return false, nil
        } else {
            if ls.isDelayed {
                time.Sleep(time.Duration(ls.msDelay) * time.Millisecond)
            }
            // Write the full framed message
            _, err := ls.s.Write(fullMsg)
            if err != nil {
                return false, errors.New("Send Write error: " + err.Error())
            }
            return true, nil
        }
    }
    return false, errors.New("Send failed, nil socket")
}

// receive a byte-string over the socket connection.
// no significant change to normal socket receive.
// 
//Failure scenario when designing: 
// 1. Partial header reads, where only 1 or 2 bytes of the 4byte Big endian 
// length header arrive in the first TCP segment due to network congestion
// 2. Payload truncation, where a reported 5000 bytes message only arrives in 2000bytes
// 3. The remote peer, the Caller, crashes or closes the connection immediately after sending the request, or the network link is severed.
func (ls *LeakySocket) Recv() ([]byte, error) {
	if ls.s == nil {
        return nil, errors.New("Recv failed, nil socket")
    }

	header := make([]byte, 4)
    _, err := io.ReadFull(ls.s, header)
    if err != nil {
        return nil, err
    }

    // 2. Parse the length
    dataLen := binary.BigEndian.Uint32(header)

    // 3. Read exactly that many bytes
    data := make([]byte, dataLen)
    _, err = io.ReadFull(ls.s, data)
    if err != nil {
        return nil, err
    }

    return data, nil
}


// enable/disable emulated transmission delay and/or change the delay parameter
func (ls *LeakySocket) SetDelay(delayed bool, ms int, us int) {
    ls.isDelayed = delayed
    ls.msDelay = ms
    ls.usDelay = us
}

// change the emulated timeout period used with packet loss
func (ls *LeakySocket) SetTimeout(ms int, us int) {
    ls.msTimeout = ms
    ls.usTimeout = us
}

// enable/disable emulated packet loss and/or change the loss rate
func (ls *LeakySocket) SetLossRate(lossy bool, rate float32) {
    ls.isLossy = lossy
    ls.lossRate = rate
}

// close the socket (can also be done on original net.Conn passed to builder)
func (ls *LeakySocket) Close() error {
    return ls.s.Close()
}


// RemoteError

// RemoteError is a custom error type used for this library to identify remote methods.
// it is used by both caller and callee endpoints.
type RemoteError struct { 
    Err string 
}

// getter for the error message included inside the custom error type
func (e *RemoteError) Error() string {
    return e.Err
}


// RequestMsg (this is only a suggestion, can be changed)
//
// RequestMsg represents the request message sent from caller to callee.
// it is used by both endpoints, and uses the reflect package to carry
// arbitrary argument types across the network.
type RequestMsg struct {
	Method string
	Args   []reflect.Value
}

// ReplyMsg (this is only a suggestion, can be changed)
//
// ReplyMsg represents the reply message sent from callee back to caller
// in response to a RequestMsg. it similarly uses reflection to carry
// arbitrary return types along with a success indicator to tell the caller
// whether the call was correctly handled by the callee. also includes
// a RemoteError to specify details of any encountered failure.
type ReplyMsg struct {
	Success bool
	Reply   []reflect.Value
	Err     RemoteError
}


// CalleeStub -- stub that receives remote calls and hosts an object/instance
//
// A CalleeStub encapsulates a multithreaded TCP server that manages a single
// remote object on a single TCP port, which is a simplification to ease management
// of remote objects and interaction with callers.  Each CalleeStub is built
// around a single struct of function declarations. All remote calls are
// handled synchronously, meaning the lifetime of a connection is that of a
// sinngle method call.  A CalleeStub can encounter a number of different issues,
// and most of them will result in sending a failure response to the caller,
// including a RemoteError with suitable details.
type CalleeStub struct {
	//       - reflect.Type of the CalleeStub's service interface (struct of Fields)
	//       - reflect.Value of the CalleeStub's service interface
	//       - reflect.Value of the CalleeStub's remote object instance
	//       - status and configuration parameters, as needed
	ifaceType  reflect.Type
	ifaceValue reflect.Value
	objValue   reflect.Value
	address    string
	isLossy    bool
	isDelayed  bool
	mu         sync.Mutex
	listener   net.Listener
	running    bool
	callCount  int
	stopChan   chan struct{}

}

// Callee defines the minimum contract our
// CalleeStub implementation must satisfy.
type Callee interface {
	Start() error           // start a TCP server, then return
	Stop() error            // close the TCP server, then return
	IsRunning() bool        // is the TCP server running?
	GetCallCount() int      // how many calls has the TCP server handled (across restarts)?
}
func (c *CalleeStub) IsRunning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running
}

func (c *CalleeStub) GetCallCount() int {
	c.mu.Lock()
    defer c.mu.Unlock()
	return c.callCount
}
// app specific

func init() {
    gob.Register(RemoteError{})
	gob.Register(StockOrder{})
}

func convert(val reflect.Value, targetType reflect.Type) reflect.Value {
    if !val.IsValid() {
        return reflect.Zero(targetType)
    }

    // Unpack interface if the value is held inside one
    if val.Kind() == reflect.Interface && !val.IsNil() {
        val = val.Elem()
    }

    // Direct match or convertible
    if val.Type().ConvertibleTo(targetType) {
        return val.Convert(targetType)
    }

    // Handle Slice conversion
    if val.Kind() == reflect.Slice && targetType.Kind() == reflect.Slice {
        newSlice := reflect.MakeSlice(targetType, val.Len(), val.Len())
        for i := 0; i < val.Len(); i++ {
            // Recursively convert each ele
            convertedItem := convert(val.Index(i), targetType.Elem())
            newSlice.Index(i).Set(convertedItem)
        }
        return newSlice
    }

    // Handle Map conversion
    if val.Kind() == reflect.Map && targetType.Kind() == reflect.Map {
        newMap := reflect.MakeMap(targetType)
        for _, key := range val.MapKeys() {
            // Recursively convert key and value
            convertedKey := convert(key, targetType.Key())
            convertedVal := convert(val.MapIndex(key), targetType.Elem())
			if !convertedKey.IsValid() || !convertedVal.IsValid() { return reflect.Value{} }
            newMap.SetMapIndex(convertedKey, convertedVal)
        }
        return newMap
    }

    return reflect.Value{}
}

func (c *CalleeStub) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running {
		return errors.New("already running")
	}
	l, err := net.Listen("tcp", c.address)
	if err != nil {
		return err
	}
	c.listener = l
	c.running = true
	c.stopChan = make(chan struct{}) // RE-INITIALIZE to allow restart
	stopCh := c.stopChan
    listener := c.listener

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
                case <-stopCh:
                    return
                default:
                    // transient accept error, keep looping
                    continue
                }
			}
			go c.handleConnection(conn)
		}
	}()
	return nil
}

func (c *CalleeStub) handleConnection(conn net.Conn) {
	defer conn.Close()
    ls := NewLeakySocket(conn, c.isLossy, c.isDelayed)
	
    data, err := ls.Recv()
    if err != nil { return }
	defer func() {
		if r := recover(); r != nil {
			c.sendReply(ls, false, nil, RemoteError{Err: fmt.Sprintf("Callee Panic: %v", r)})
		}
	}()

    var req struct {
        Method string
        Args   []interface{}
    }
    
    decoder := gob.NewDecoder(bytes.NewReader(data))
    if err := decoder.Decode(&req); err != nil { 
		return 
	}

//
    method := c.objValue.MethodByName(req.Method)
	

    if !method.IsValid() {
        // Handle mismatched interface
        c.sendReply(ls, false, nil, RemoteError{Err: "Method not found"})
        return
    }

	if len(req.Args) != method.Type().NumIn() {
		c.sendReply(ls, false, nil, RemoteError{Err: "Arg count mismatch"})
		return
	}

    // Convert interface args back
    in := make([]reflect.Value, len(req.Args))
    for i, arg := range req.Args {
        expectedType := method.Type().In(i)
		receivedVal := reflect.ValueOf(arg)
        // Use recursive convert to handle nested slices/maps from gob
        convertedArg := convert(receivedVal, expectedType)
    
		if !convertedArg.IsValid() {
			errMsg := fmt.Sprintf("Type mismatch for %s: expected %s, got %T", req.Method, expectedType, arg)
			c.sendReply(ls, false, nil, RemoteError{Err: errMsg})
			return
		}
		in[i] = convertedArg
    }

    // Guard against argument mismatch
    if len(in) != method.Type().NumIn() {
        c.sendReply(ls, false, nil, RemoteError{Err: "Arg count mismatch"})
        return
    }

    results := method.Call(in)
	
    c.mu.Lock()
    c.callCount++
    c.mu.Unlock()

    out := make([]interface{}, len(results))
    for i, res := range results {
        out[i] = res.Interface()
	}

    c.sendReply(ls, true, out, RemoteError{})
}

func (c *CalleeStub) sendReply(ls *LeakySocket, success bool, reply []interface{}, rErr RemoteError) {
    var resp struct {
        Success bool
        Reply   []interface{}
        Err     RemoteError
    }
    resp.Success = success
    resp.Reply = reply
    resp.Err = rErr

    var buf bytes.Buffer
    encoder := gob.NewEncoder(&buf)
    encoder.Encode(resp)
    ls.Send(buf.Bytes())
}



func (c *CalleeStub) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.running {
		return nil
	}
	c.running = false
	close(c.stopChan)
	if c.listener != nil {
		return c.listener.Close()
	}
	return nil
}


// build a new CalleeStub instance around a given struct of supported functions,
// a local instance of a corresponding object that supports these functions,
// and arguments to support creation and use of LeakySocket-wrapped connections.
// performs the following:
// -- returns a local error if function struct or object is nil
// -- returns a local error if any function in the struct is not a remote function
// -- if neither error, creates and populates a CalleeStub and returns a pointer
//
// Failure Scenarios: Returns an error if the interface or object is nil,
// or if method signatures don't return remote.RemoteError.
func NewCalleeStub(sv interface{}, sobj interface{}, address string, lossy bool, delayed bool) (Callee, error) {
	// if ifc is a pointer to a struct with function declarations,
	// then reflect.TypeOf(ifc).Elem() is the reflected struct's Type

	// if sobj is a pointer to an object instance, then
	// reflect.ValueOf(sobj) is the reflected object's Value

	if sv == nil || sobj == nil {
		return nil, errors.New("interface or object cannot be nil")
	}

	svType := reflect.TypeOf(sv).Elem()
	if !isValidRemoteInterface(svType) {
		return nil, errors.New("invalid remote interface: methods must return RemoteError")
	}

	return &CalleeStub{
		ifaceType:  svType,
		ifaceValue: reflect.ValueOf(sv).Elem(),
		objValue:   reflect.ValueOf(sobj),
		address:    address,
		isLossy:    lossy,
		isDelayed:  delayed,
		stopChan:   make(chan struct{}),
	}, nil
}

func gobRegisterRecursive(x interface{}) {
	if x == nil {
		return
	}
	gob.Register(x)

	v := reflect.ValueOf(x)
	if v.Kind() == reflect.Interface && !v.IsNil() {
		v = v.Elem()
	}
	if !v.IsValid() {
		return
	}

	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			gobRegisterRecursive(v.Index(i).Interface())
		}
	case reflect.Map:
		for _, k := range v.MapKeys() {
			gobRegisterRecursive(k.Interface())
			gobRegisterRecursive(v.MapIndex(k).Interface())
		}
	}
}

// CallerStubCreator -- use reflection to populate the interface functions to create the
// caller's stub interface. Only works if all functions are exported. Once created, 
// the interface masks remote calls to a CalleeStub that hosts the object instance that 
// the functions are invoked on.  The network address of the remote CalleeStub must be 
// provided with the stub is created, and it may not change later. Arguments include:
// -- a struct of function declarations to act as the stub's interface
// -- the remote address of the CalleeStub as "<ip-address>:<port-number>"
// -- indicator of whether caller-to-callee channel has emulated packet loss
// -- indicator of whether caller-to-callee channel has emulated propagation delay
// Performs the following:
// -- returns a local error if function struct is nil
// -- returns a local error if any function in the struct is not a remote function
// -- otherwise, uses reflection to access the functions in the given struct and
//	  populate their function definitions with the required CallerStub functionality
//
// Failure scenarios when designing : 
// Returns an error if the interface is nil or invalid. 
// Proxied methods return RemoteError if the network is unreachble or a packet is dropped.
func CallerStubCreator(ifc interface{}, adr string, lossy bool, delayed bool) error {
	// if ifc is a pointer to a struct with function declarations,
	// then reflect.TypeOf(ifc).Elem() is the reflected struct's reflect.Type
	// and reflect.ValueOf(ifc).Elem() is the reflected struct's reflect.Value
	//
	// Here's what it needs to do (not strictly in this order):
	//
	//    1. create a request message populated with the method name and input
	//       arguments to send to the CalleeStub
	//
	//    2. create a []reflect.Value of correct size to hold the result to be
	//       returned back to the program
	//
	//    3. connect to the CalleeStub's tcp server, and wrap the connection in an
	//       appropriate LeakySocket using the parameters given to the CallerStubCreator
	//
	//    4. encode the request message into a byte-string to send over the connection
	//
	//    5. send the encoded message, noting that the LeakySocket is not guaranteed
	//       to succeed depending on the given parameters
	//
	//    6. wait for a reply to be received using Recv, which is blocking
	//        -- if Recv returns an error, populate and return error output
	//
	//    7. decode the received byte-string according to the expected return types
	if ifc == nil {
		return errors.New("nil interface")
	}
	v := reflect.ValueOf(ifc).Elem()
	t := v.Type()
	if !isValidRemoteInterface(t) {
		return errors.New("invalid interface")
	}

	for i := 0; i < t.NumField(); i++ {
		fName, fType := t.Field(i).Name, t.Field(i).Type
		v.Field(i).Set(reflect.MakeFunc(fType, func(args []reflect.Value) []reflect.Value {
			// Handles LossyConnection packet drops transparently
			for attempt := 0; attempt < 10; attempt++ {
				conn, err := net.Dial("tcp", adr)
				if err != nil {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				ls := NewLeakySocket(conn, lossy, delayed)

				reqArgs := make([]interface{}, len(args))
				for j := range args {
					reqArgs[j] = args[j].Interface()

					gobRegisterRecursive(reqArgs[j])
				}

				req := struct {
					Method string
					Args   []interface{}
				}{Method: fName, Args: reqArgs}

				var buf bytes.Buffer
				enc := gob.NewEncoder(&buf)
				// Check for encoding errors
				if err := enc.Encode(req); err != nil {
					conn.Close()
					return generateErrorValues(fType, "Gob encode error: "+err.Error())
				}

				if ok, _ := ls.Send(buf.Bytes()); !ok {
					conn.Close()
					continue
				}
				respData, err := ls.Recv()
				if err != nil {
					conn.Close()
					continue
				}

				var resp struct {
					Success bool
					Reply   []interface{}
					Err     RemoteError
				}
				if err := gob.NewDecoder(bytes.NewReader(respData)).Decode(&resp); err != nil {
					conn.Close()
					continue
				}

				conn.Close()
				// out := make([]reflect.Value, fType.NumOut())

				// HANDLE FAIL: If callee reported success=false
				if !resp.Success {
					return generateErrorValues(fType, resp.Err.Error())
				}

				// HANDLE SUC: Convert return values (all except the last RemoteError)

				if len(resp.Reply) != fType.NumOut() {
					return generateErrorValues(
						fType,
						fmt.Sprintf("Reply arity mismatch: expected %d values, got %d", fType.NumOut(), len(resp.Reply)),
					)
				}

				out := make([]reflect.Value, fType.NumOut())

				// Convert every return value including the last RemoteError.
				for j := 0; j < fType.NumOut(); j++ {
					expectedType := fType.Out(j)
					receivedVal := reflect.ValueOf(resp.Reply[j])

					convertedVal := convert(receivedVal, expectedType)
					if !convertedVal.IsValid() {
						return generateErrorValues(
							fType,
							fmt.Sprintf("Return type mismatch at index %d: expected %s, got %T",
								j, expectedType, resp.Reply[j]),
						)
					}
					out[j] = convertedVal
				}
				return out



			}
			return generateErrorValues(fType, "RPC timeout after retries")
		}))
	}
	return nil
}

func generateErrorValues(t reflect.Type, msg string) []reflect.Value {
    out := make([]reflect.Value, t.NumOut())
    for i := 0; i < t.NumOut()-1; i++ {
        out[i] = reflect.Zero(t.Out(i))
    }
    out[t.NumOut()-1] = reflect.ValueOf(RemoteError{Err: msg})
    return out
}