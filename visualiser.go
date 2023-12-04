package workflow

import (
	"os"
	"strings"
	"text/template"
)

func MermaidDiagram[Type any, Status ~string](w *Workflow[Type, Status], path string, d MermaidDirection) error {
	breakDown := strings.Split(path, "/")
	dirPath := strings.Join(breakDown[:len(breakDown)-1], "/")

	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		return err
	}

	fileName := breakDown[len(breakDown)-1:][0]
	file, err := os.Create(dirPath + "/" + fileName)
	if err != nil {
		return err
	}

	if d == UnknownDirection {
		d = LeftToRightDirection
	}

	mf := MermaidFormat{
		Direction: d,
	}

	startingPoint := make(map[Status]bool)
	for _, from := range w.graphOrder {
		if _, ok := startingPoint[from]; !ok {
			startingPoint[from] = true
		}

		if w.endPoints[from] {
			mf.TerminalPoints = append(mf.TerminalPoints, string(from))
		}

		for _, to := range w.graph[from] {
			startingPoint[to] = false

			mf.Transitions = append(mf.Transitions, MermaidTransition{
				From: string(from),
				To:   string(to),
			})

			if w.endPoints[to] {
				mf.TerminalPoints = append(mf.TerminalPoints, string(to))
			}
		}

	}

	for _, from := range w.graphOrder {
		if !startingPoint[from] {
			continue
		}

		mf.StartingPoints = append(mf.StartingPoints, string(from))
	}

	return template.Must(template.New("").Parse("```"+mermaidTemplate+"```")).Execute(file, mf)
}

type MermaidFormat struct {
	Direction      MermaidDirection
	StartingPoints []string
	TerminalPoints []string
	Transitions    []MermaidTransition
}

type MermaidDirection string

const (
	UnknownDirection     MermaidDirection = ""
	TopToBottomDirection MermaidDirection = "TB"
	LeftToRightDirection MermaidDirection = "LR"
	RightToLeftDirection MermaidDirection = "RL"
	BottomToTopDirection MermaidDirection = "BT"
)

type MermaidTransition struct {
	From string
	To   string
}

var mermaidTemplate = `mermaid
stateDiagram-v2
	direction {{.Direction}}
	{{range $key, $value := .StartingPoints }}
	[*]-->{{$value}}
	{{- end }}
	{{range $key, $value := .Transitions }}
	{{$value.From}}-->{{$value.To}}
	{{- end }}
	{{range $key, $value := .TerminalPoints }}
	{{$value}}-->[*]
	{{- end }}
`
