package main

import (
	"context"
	"fmt"
	"time"

	"github.com/NimbleMarkets/ntcharts/linechart/timeserieslinechart"
	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/harmonica"
	"github.com/charmbracelet/lipgloss"
	"github.com/dtpu/searchengine/crawler/structs"
	zone "github.com/lrstanley/bubblezone"
	"github.com/nats-io/nats.go"
)

// CrawlerState represents the current state of the crawler
type CrawlerState int

const (
	StateStopped CrawlerState = iota
	StateRunning
	StatePaused
)

// model holds the TUI state
type model struct {
	// Crawler state
	state      CrawlerState
	stats      structs.Stats
	lastUpdate time.Time

	// UI components
	spinner     spinner.Model
	zoneManager *zone.Manager
	chart       timeserieslinechart.Model

	// Animation springs for smooth counter transitions
	springPagesCrawled harmonica.Spring
	springPagesFailed  harmonica.Spring
	springLinksFound   harmonica.Spring
	springQueueSize    harmonica.Spring

	// Animated values for display
	animPagesCrawled float64
	animPagesFailed  float64
	animLinksFound   float64
	animQueueSize    float64

	// Velocities for spring animation
	velPagesCrawled float64
	velPagesFailed  float64
	velLinksFound   float64
	velQueueSize    float64

	// Window size
	width  int
	height int

	// Time series data history
	crawlHistory []timePoint
	maxHistory   int

	// Crawler control
	statsChan  chan structs.Stats
	ctx        context.Context
	cancelFunc context.CancelFunc
	errorMsg   string
}

type timePoint struct {
	time  time.Time
	value float64
}

// Messages
type tickMsg time.Time
type statsUpdateMsg structs.Stats
type stateChangeMsg CrawlerState

// checkNATSConnection verifies that NATS server is reachable
func checkNATSConnection() error {
	nc, err := nats.Connect("nats://localhost:4222", nats.Timeout(2*time.Second))
	if err != nil {
		return err
	}
	defer nc.Close()
	return nil
}

// waitForStatsUpdate creates a command that waits for stats updates from the channel
func waitForStatsUpdate(statsChan <-chan structs.Stats) tea.Cmd {
	return func() tea.Msg {
		select {
		case stats := <-statsChan:
			return statsUpdateMsg(stats)
		case <-time.After(5 * time.Second):
			// Timeout to prevent blocking forever
			return nil
		}
	}
}

func initialModel() model {
	sp := spinner.New()
	sp.Spinner = spinner.Dot
	sp.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))

	// Initialize time series chart
	chart := timeserieslinechart.New(60, 5,
		timeserieslinechart.WithXLabelFormatter(timeserieslinechart.HourTimeLabelFormatter()),
	)
	chart.AxisStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
	chart.LabelStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("246"))
	chart.DrawBraille()

	ctx, cancel := context.WithCancel(context.Background())

	return model{
		state:       StateStopped,
		spinner:     sp,
		zoneManager: zone.New(),
		chart:       chart,
		maxHistory:  60,
		statsChan:   make(chan structs.Stats, 100),
		ctx:         ctx,
		cancelFunc:  cancel,

		// Initialize springs with critically damped settings (damping=1.0) for smooth, professional transitions
		// Frequency of 8.0 provides snappy updates
		springPagesCrawled: harmonica.NewSpring(harmonica.FPS(60), 8.0, 1.0),
		springPagesFailed:  harmonica.NewSpring(harmonica.FPS(60), 8.0, 1.0),
		springLinksFound:   harmonica.NewSpring(harmonica.FPS(60), 8.0, 1.0),
		springQueueSize:    harmonica.NewSpring(harmonica.FPS(60), 8.0, 1.0),
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(
		m.spinner.Tick,
		tickCmd(),
	)
}

func tickCmd() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height

		// Resize chart to fit window
		chartWidth := min(m.width-4, 100)
		chartHeight := min(m.height/2-4, 5)
		m.chart = timeserieslinechart.New(chartWidth, chartHeight,
			timeserieslinechart.WithXLabelFormatter(timeserieslinechart.HourTimeLabelFormatter()),
		)
		m.chart.AxisStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
		m.chart.LabelStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("246"))
		m.chart.DrawBraille()

		// Repopulate chart with history
		for _, pt := range m.crawlHistory {
			m.chart.Push(timeserieslinechart.TimePoint{
				Time:  pt.time,
				Value: pt.value,
			})
		}

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		case " ":
			// Toggle pause/resume (force stop and restart)
			if m.state == StateRunning {
				m.state = StatePaused
				if m.cancelFunc != nil {
					m.cancelFunc()
				}
			} else if m.state == StatePaused {
				if err := checkNATSConnection(); err != nil {
					m.errorMsg = fmt.Sprintf("NATS connection failed: %v", err)
				} else {
					m.errorMsg = ""
					m.state = StateRunning
					m.ctx, m.cancelFunc = context.WithCancel(context.Background())
					//go startCrawler(m.ctx, m.statsChan)
					cmds = append(cmds, waitForStatsUpdate(m.statsChan))
				}
			}
		case "s":
			// Start crawler
			if m.state == StateStopped {
				if err := checkNATSConnection(); err != nil {
					m.errorMsg = fmt.Sprintf("NATS connection failed: %v", err)
				} else {
					m.errorMsg = ""
					m.state = StateRunning
					m.ctx, m.cancelFunc = context.WithCancel(context.Background())
					//go startCrawler(m.ctx, m.statsChan)
					cmds = append(cmds, waitForStatsUpdate(m.statsChan))
				}
			}
		case "r":
			// Restart crawler
			if m.cancelFunc != nil {
				m.cancelFunc()
			}
			m.stats = structs.Stats{}
			m.crawlHistory = nil
			m.animPagesCrawled = 0
			m.animPagesFailed = 0
			m.animLinksFound = 0
			m.animQueueSize = 0
			if err := checkNATSConnection(); err != nil {
				m.errorMsg = fmt.Sprintf("NATS connection failed: %v", err)
				m.state = StateStopped
			} else {
				m.errorMsg = ""
				m.state = StateRunning
				m.ctx, m.cancelFunc = context.WithCancel(context.Background())
				//go startCrawler(m.ctx, m.statsChan)
				cmds = append(cmds, waitForStatsUpdate(m.statsChan))
			}
		}

	case tea.MouseMsg:
		if msg.Action == tea.MouseActionRelease && msg.Button == tea.MouseButtonLeft {
			if m.zoneManager.Get("btn-start").InBounds(msg) && m.state == StateStopped {
				if err := checkNATSConnection(); err != nil {
					m.errorMsg = fmt.Sprintf("NATS connection failed: %v", err)
				} else {
					m.errorMsg = ""
					m.state = StateRunning
					m.ctx, m.cancelFunc = context.WithCancel(context.Background())
					//go startCrawler(m.ctx, m.statsChan)
					cmds = append(cmds, waitForStatsUpdate(m.statsChan))
				}
			}
			if m.zoneManager.Get("btn-pause").InBounds(msg) && m.state == StateRunning {
				m.state = StatePaused
				if m.cancelFunc != nil {
					m.cancelFunc()
				}
			}
			if m.zoneManager.Get("btn-resume").InBounds(msg) && m.state == StatePaused {
				if err := checkNATSConnection(); err != nil {
					m.errorMsg = fmt.Sprintf("NATS connection failed: %v", err)
				} else {
					m.errorMsg = ""
					m.state = StateRunning
					m.ctx, m.cancelFunc = context.WithCancel(context.Background())
					//go startCrawler(m.ctx, m.statsChan)
					cmds = append(cmds, waitForStatsUpdate(m.statsChan))
				}
			}
			if m.zoneManager.Get("btn-restart").InBounds(msg) {
				if m.cancelFunc != nil {
					m.cancelFunc()
				}
				m.stats = structs.Stats{}
				m.crawlHistory = nil
				m.animPagesCrawled = 0
				m.animPagesFailed = 0
				m.animLinksFound = 0
				m.animQueueSize = 0
				if err := checkNATSConnection(); err != nil {
					m.errorMsg = fmt.Sprintf("NATS connection failed: %v", err)
					m.state = StateStopped
				} else {
					m.errorMsg = ""
					m.state = StateRunning
					m.ctx, m.cancelFunc = context.WithCancel(context.Background())
					//go startCrawler(m.ctx, m.statsChan)
					cmds = append(cmds, waitForStatsUpdate(m.statsChan))
				}
			}
		}

	case tickMsg:
		// Update animated values using spring physics
		m.animPagesCrawled, m.velPagesCrawled = m.springPagesCrawled.Update(
			m.animPagesCrawled,
			m.velPagesCrawled,
			float64(m.stats.PagesCrawled),
		)
		m.animPagesFailed, m.velPagesFailed = m.springPagesFailed.Update(
			m.animPagesFailed,
			m.velPagesFailed,
			float64(m.stats.PagesFailed),
		)
		m.animLinksFound, m.velLinksFound = m.springLinksFound.Update(
			m.animLinksFound,
			m.velLinksFound,
			float64(m.stats.LinksFound),
		)
		m.animQueueSize, m.velQueueSize = m.springQueueSize.Update(
			m.animQueueSize,
			m.velQueueSize,
			float64(m.stats.QueueSize),
		)

		cmds = append(cmds, tickCmd())

	case statsUpdateMsg:
		m.stats = structs.Stats(msg)
		m.lastUpdate = time.Now()

		// Add to time series history
		pt := timePoint{
			time:  m.lastUpdate,
			value: float64(m.stats.PagesCrawled),
		}
		m.crawlHistory = append(m.crawlHistory, pt)

		// Keep only last maxHistory points
		if len(m.crawlHistory) > m.maxHistory {
			m.crawlHistory = m.crawlHistory[len(m.crawlHistory)-m.maxHistory:]
		}

		// Update chart
		m.chart.Push(timeserieslinechart.TimePoint{
			Time:  pt.time,
			Value: pt.value,
		})

		// Continue waiting for more stats updates
		if m.state == StateRunning {
			cmds = append(cmds, waitForStatsUpdate(m.statsChan))
		}

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		cmds = append(cmds, cmd)

	default:
		// For nil messages (like timeout from waitForStatsUpdate), keep polling if running
		if m.state == StateRunning {
			cmds = append(cmds, waitForStatsUpdate(m.statsChan))
		}
	}

	return m, tea.Batch(cmds...)
}

func (m model) View() string {
	if m.width == 0 {
		return "Initializing..."
	}

	// Define color palette
	var (
		successColor = lipgloss.AdaptiveColor{Light: "#00AA00", Dark: "#00FF00"}
		errorColor   = lipgloss.AdaptiveColor{Light: "#AA0000", Dark: "#FF4444"}
		infoColor    = lipgloss.AdaptiveColor{Light: "#0066AA", Dark: "#66AAFF"}
		warningColor = lipgloss.AdaptiveColor{Light: "#AA6600", Dark: "#FFAA44"}
		accentColor  = lipgloss.AdaptiveColor{Light: "#6600AA", Dark: "#AA66FF"}
		dimColor     = lipgloss.AdaptiveColor{Light: "#666666", Dark: "#999999"}
	)

	// Header with title and status
	titleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("86")).
		Padding(0, 1)

	statusStyle := lipgloss.NewStyle().
		Foreground(dimColor).
		Padding(0, 1)

	var statusText string
	switch m.state {
	case StateRunning:
		statusText = fmt.Sprintf("%s Running", m.spinner.View())
	case StatePaused:
		statusText = "⏸  Paused"
	case StateStopped:
		statusText = "⏹  Stopped"
	}

	header := lipgloss.JoinHorizontal(
		lipgloss.Top,
		titleStyle.Render("🕷️  Web Crawler"),
		statusStyle.Render(statusText),
	)

	// Stats panels
	statPanelStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		Padding(1, 2).
		Width(22)

	successPanel := statPanelStyle.Copy().
		BorderForeground(successColor).
		Render(fmt.Sprintf("✓ Pages Crawled\n\n%s",
			lipgloss.NewStyle().
				Foreground(successColor).
				Bold(true).
				Render(fmt.Sprintf("%d", int(m.animPagesCrawled)))))

	errorPanel := statPanelStyle.Copy().
		BorderForeground(errorColor).
		Render(fmt.Sprintf("✗ Pages Failed\n\n%s",
			lipgloss.NewStyle().
				Foreground(errorColor).
				Bold(true).
				Render(fmt.Sprintf("%d", int(m.animPagesFailed)))))

	queuePanel := statPanelStyle.Copy().
		BorderForeground(infoColor).
		Render(fmt.Sprintf("⏳ Queue Size\n\n%s",
			lipgloss.NewStyle().
				Foreground(infoColor).
				Bold(true).
				Render(fmt.Sprintf("%d", int(m.animQueueSize)))))

	linksPanel := statPanelStyle.Copy().
		BorderForeground(warningColor).
		Render(fmt.Sprintf("🔗 Links Found\n\n%s",
			lipgloss.NewStyle().
				Foreground(warningColor).
				Bold(true).
				Render(fmt.Sprintf("%d", int(m.animLinksFound)))))

	workersPanel := statPanelStyle.Copy().
		BorderForeground(accentColor).
		Render(fmt.Sprintf("⚙️  Active Workers\n\n%s",
			lipgloss.NewStyle().
				Foreground(accentColor).
				Bold(true).
				Render(fmt.Sprintf("%d", m.stats.ActiveWorkers))))

	statsRow := lipgloss.JoinHorizontal(
		lipgloss.Top,
		successPanel,
		errorPanel,
		queuePanel,
		linksPanel,
		workersPanel,
	)

	// Time series chart
	chartStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("240")).
		Padding(1, 2).
		MarginTop(1)

	chartTitle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("246")).
		Bold(true).
		Render("📈 Pages Crawled Over Time")

	chartView := m.chart.View()
	chartPanel := chartStyle.Render(chartTitle + "\n" + chartView)

	// Control buttons
	buttonStyle := lipgloss.NewStyle().
		Padding(0, 2).
		Border(lipgloss.RoundedBorder()).
		MarginRight(1)

	activeButtonStyle := buttonStyle.Copy().
		Foreground(lipgloss.Color("0")).
		Background(lipgloss.Color("86")).
		Bold(true)

	disabledButtonStyle := buttonStyle.Copy().
		Foreground(dimColor).
		BorderForeground(dimColor)

	var startBtn, pauseBtn, restartBtn, quitBtn string

	if m.state == StateStopped {
		startBtn = m.zoneManager.Mark("btn-start", activeButtonStyle.Render("Start (s)"))
		pauseBtn = disabledButtonStyle.Render("Pause")
	} else if m.state == StateRunning {
		startBtn = disabledButtonStyle.Render("Start")
		pauseBtn = m.zoneManager.Mark("btn-pause", activeButtonStyle.Render("Pause (space)"))
	} else {
		startBtn = disabledButtonStyle.Render("Start")
		pauseBtn = m.zoneManager.Mark("btn-resume", activeButtonStyle.Render("Resume (space)"))
	}

	restartBtn = m.zoneManager.Mark("btn-restart", buttonStyle.Copy().
		BorderForeground(warningColor).
		Foreground(warningColor).
		Render("Restart (r)"))

	quitBtn = buttonStyle.Copy().
		BorderForeground(errorColor).
		Foreground(errorColor).
		Render("Quit (q)")

	controls := lipgloss.NewStyle().
		MarginTop(1).
		Render(lipgloss.JoinHorizontal(lipgloss.Top, startBtn, pauseBtn, restartBtn, quitBtn))

	// Error message if any
	errorView := ""
	if m.errorMsg != "" {
		errorStyle := lipgloss.NewStyle().
			Foreground(errorColor).
			MarginTop(1).
			Padding(0, 1)
		errorView = errorStyle.Render("⚠️  " + m.errorMsg)
	}

	// Combine all sections
	view := lipgloss.JoinVertical(
		lipgloss.Left,
		header,
		statsRow,
		chartPanel,
		controls,
		errorView,
	)

	return m.zoneManager.Scan(view)
}

func runTUI() error {
	p := tea.NewProgram(
		initialModel(),
		tea.WithAltScreen(),
		tea.WithMouseCellMotion(),
	)

	zone.NewGlobal()

	if _, err := p.Run(); err != nil {
		return fmt.Errorf("error running TUI: %w", err)
	}

	return nil
}
