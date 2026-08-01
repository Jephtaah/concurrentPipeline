package pipeline

import "sync"


type Pauser struct {
	mu     sync.Mutex
	paused bool
	resume chan struct{} 
}


func NewPauser() *Pauser {
	p := &Pauser{resume: make(chan struct{})}
	close(p.resume) 
	return p
}

func (p *Pauser) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.paused {
		return
	}
	p.paused = true
	p.resume = make(chan struct{})
}

func (p *Pauser) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.paused {
		return
	}
	p.paused = false
	close(p.resume)
}

func (p *Pauser) Wait() {
	p.mu.Lock()
	ch := p.resume
	p.mu.Unlock()
	<-ch
}