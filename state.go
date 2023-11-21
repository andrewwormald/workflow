package workflow

type State string

const (
	StateUnknown  State = ""
	StateIdle     State = "Idle"
	StateRunning  State = "Running"
	StateShutdown State = "Shutdown"
)

func (w *Workflow[Type, Status]) updateState(role string, s State) {
	w.internalStateMu.Lock()
	defer w.internalStateMu.Unlock()

	w.internalState[role] = s
}

func (w *Workflow[Type, Status]) States() map[string]State {
	w.internalStateMu.Lock()
	defer w.internalStateMu.Unlock()

	return w.internalState
}
