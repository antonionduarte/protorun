package transport

import "bytes"

type Layer interface {
	Connect(host Host)
	Disconnect(host Host)
	Send(msg Message, sendTo Host)

	OutChannel() chan Message
	OutEvents() chan Event

	Cancel()
}

type (
	Message struct {
		Host Host
		Msg  bytes.Buffer
	}

	Event interface {
		Host() Host
	}

	Connected struct {
		host Host
	}

	Disconnected struct {
		host Host
	}

	Failed struct {
		host Host
	}
)

func NewMessage(msg bytes.Buffer, host Host) Message {
	return Message{Msg: msg, Host: host}
}

func (e *Connected) Host() Host    { return e.host }
func (e *Disconnected) Host() Host { return e.host }
func (e *Failed) Host() Host       { return e.host }
