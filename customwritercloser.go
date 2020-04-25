package main

type customWriteCloser struct {
	writeDelegate func(p []byte) (n int, err error)
	closeDelegate func() error 
}

func (cwc customWriteCloser) Close() error {
	return cwc.closeDelegate()
}

func (cwc customWriteCloser) Write(p []byte) (n int, err error) {
	return cwc.writeDelegate(p)
}


