package dingtalk

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/linjiongxin/go-tiny-claw/internal/engine"
	"github.com/linjiongxin/go-tiny-claw/internal/platform"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/chatbot"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/client"
)

// DingTalkBot 封装了钉钉 Stream 模式机器人的配置与核心业务流
type DingTalkBot struct {
	appKey    string
	appSecret string
	tokenMgr  *AccessTokenManager
	engine    *engine.AgentEngine
}

func NewDingTalkBot(eng *engine.AgentEngine) *DingTalkBot {
	appKey := os.Getenv("DINGTALK_APP_KEY")
	appSecret := os.Getenv("DINGTALK_APP_SECRET")

	bot := &DingTalkBot{
		appKey:    appKey,
		appSecret: appSecret,
		engine:    eng,
	}
	if appKey != "" && appSecret != "" {
		bot.tokenMgr = NewAccessTokenManager(appKey, appSecret)
	}
	return bot
}

// Start 启动钉钉 Stream 长连接（阻塞方法，需在 goroutine 中调用）
func (b *DingTalkBot) Start(ctx context.Context) error {
	cli := client.NewStreamClient(
		client.WithAppCredential(client.NewAppCredentialConfig(b.appKey, b.appSecret)),
	)
	cli.RegisterChatBotCallbackRouter(b.onChatBotMessage)
	return cli.Start(ctx)
}

// onChatBotMessage 处理来自钉钉的机器人消息。
// 和飞书的 EventDispatcher 回调职责一致：提取内容、异步派发、立即返回 ACK。
func (b *DingTalkBot) onChatBotMessage(ctx context.Context, data *chatbot.BotCallbackDataModel) ([]byte, error) {
	content := strings.TrimSpace(data.Text.Content)
	if content == "" {
		return []byte("ok"), nil
	}

	// 【驾驭并发】：收到消息后，绝不能阻塞 Stream ACK。
	// 我们要为每个请求开启一个独立的 Goroutine 跑 Agent 任务！
	go b.handleAgentRun(data.SessionWebhook, data.SenderStaffId, content)

	return []byte("ok"), nil
}

// handleAgentRun 是连接钉钉与底层引擎的桥梁。
// 参照飞书的 handleAgentRun：内部实例化 Reporter，启动引擎，错误兜底。
func (b *DingTalkBot) handleAgentRun(sessionWebhook, userID, prompt string) {
	reporter := &DingTalkReporter{
		webhook:   sessionWebhook,
		tokenMgr:  b.tokenMgr,
		robotCode: b.appKey,
		userID:    userID,
	}

	// 先 ack，让用户知道任务已收到
	reporter.sendMsg("🤖 收到请求，开始处理...")

	// 启动引擎！
	if err := b.engine.Run(context.Background(), prompt, reporter); err != nil {
		reporter.sendMsg(fmt.Sprintf("❌ Agent 运行崩溃: %v", err))
	}
}

// ==========================================
// DingTalkReporter: 将引擎的输出格式化后发给钉钉
// ==========================================

// DingTalkReporter 通过 sessionWebhook 或 accessToken 向用户推送消息
type DingTalkReporter struct {
	webhook   string              // 优先通道：sessionWebhook（即时、高效）
	tokenMgr  *AccessTokenManager // 兜底通道：accessToken 管理器
	robotCode string              // 兜底通道：机器人编码
	userID    string              // 兜底通道：接收者用户ID
}

// sendMsg 封装了消息发送，优先用 sessionWebhook，失效时 fallback 到单聊 API。
// 和飞书的 sendMsg 职责一致，只是钉钉需要处理双通道。
func (r *DingTalkReporter) sendMsg(text string) {
	const maxLen = 4000
	display := text
	if len(display) > maxLen {
		display = display[:maxLen] + "\n\n...[内容过长已截断]..."
	}

	// 1. 优先尝试 sessionWebhook（群聊内直接回复）
	if r.webhook != "" {
		if err := r.sendViaWebhook(display); err == nil {
			return
		}
	}

	// 2. Fallback：sessionWebhook 失效时，用 accessToken 发单聊消息兜底
	if r.tokenMgr != nil && r.userID != "" && r.robotCode != "" {
		_ = r.sendViaPrivateChat(display)
	}
}

func (r *DingTalkReporter) sendViaWebhook(text string) error {
	payload := map[string]interface{}{
		"msgtype": "markdown",
		"markdown": map[string]string{
			"title": "Claw 消息",
			"text":  text,
		},
	}

	body, _ := json.Marshal(payload)
	resp, err := dingTalkHTTPClient.Post(r.webhook, "application/json", strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("webhook status: %d", resp.StatusCode)
	}
	return nil
}

func (r *DingTalkReporter) sendViaPrivateChat(text string) error {
	token, err := r.tokenMgr.GetToken()
	if err != nil {
		return fmt.Errorf("获取 accessToken 失败: %w", err)
	}

	payload := map[string]interface{}{
		"robotCode": r.robotCode,
		"userIds":   []string{r.userID},
		"msgKey":    "sampleMarkdown",
		"msgParam": map[string]string{
			"title": "Claw 消息",
			"text":  text,
		},
	}

	body, _ := json.Marshal(payload)
	req, _ := http.NewRequest(http.MethodPost, "https://api.dingtalk.com/v1.0/robot/oToMessages/batchSend", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)

	resp, err := dingTalkHTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("private chat api status: %d, body: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

func (r *DingTalkReporter) OnThinking(ctx context.Context) {
	r.sendMsg("🤔 *正在慢思考...*")
}

func (r *DingTalkReporter) OnToolCall(ctx context.Context, toolName, args string) {
	r.sendMsg(fmt.Sprintf("🛠️ **执行工具** `%s`\n\n```json\n%s\n```", toolName, args))
}

func (r *DingTalkReporter) OnToolResult(ctx context.Context, toolName, result string, isError bool) {
	if isError {
		r.sendMsg(fmt.Sprintf("⚠️ **执行报错** `%s`:\n\n```\n%s\n```", toolName, result))
	} else {
		r.sendMsg(fmt.Sprintf("✅ **执行成功** `%s`", toolName))
	}
}

func (r *DingTalkReporter) OnMessage(ctx context.Context, content string) {
	r.sendMsg(content)
}

// 编译时接口检查
var _ engine.Reporter = (*DingTalkReporter)(nil)

// =================== 平台注册 ===================

func init() {
	platform.Register(&dingTalkAdapter{})
}

type dingTalkAdapter struct{}

func (a *dingTalkAdapter) Name() string { return "dingtalk" }

func (a *dingTalkAdapter) Enabled() bool {
	return os.Getenv("DINGTALK_APP_KEY") != "" && os.Getenv("DINGTALK_APP_SECRET") != ""
}

func (a *dingTalkAdapter) Launch(ctx context.Context, mux *http.ServeMux, eng *engine.AgentEngine) error {
	bot := NewDingTalkBot(eng)
	go func() {
		if err := bot.Start(ctx); err != nil {
			log.Printf("钉钉 Stream 连接异常: %v", err)
		}
	}()
	return nil
}

// 复用 HTTP Client，避免每次新建连接
var dingTalkHTTPClient = &http.Client{
	Timeout: 10 * time.Second,
}

// =================== AccessToken 管理 ===================

// AccessTokenManager 负责缓存和自动刷新钉钉 accessToken
// 避免每次发消息都重复请求 gettoken 接口
type AccessTokenManager struct {
	appKey    string
	appSecret string
	token     string
	expiresAt time.Time
	mu        sync.RWMutex
}

func NewAccessTokenManager(appKey, appSecret string) *AccessTokenManager {
	return &AccessTokenManager{
		appKey:    appKey,
		appSecret: appSecret,
	}
}

// GetToken 获取有效的 accessToken，如果快过期则自动刷新
func (m *AccessTokenManager) GetToken() (string, error) {
	// 1. 先读锁快速检查
	m.mu.RLock()
	if m.token != "" && time.Now().Before(m.expiresAt.Add(-60*time.Second)) {
		tok := m.token
		m.mu.RUnlock()
		return tok, nil
	}
	m.mu.RUnlock()

	// 2. 需要刷新，加写锁
	m.mu.Lock()
	defer m.mu.Unlock()

	// double check
	if m.token != "" && time.Now().Before(m.expiresAt.Add(-60*time.Second)) {
		return m.token, nil
	}

	url := fmt.Sprintf("https://oapi.dingtalk.com/gettoken?appkey=%s&appsecret=%s", m.appKey, m.appSecret)
	resp, err := dingTalkHTTPClient.Get(url)
	if err != nil {
		return "", fmt.Errorf("请求 gettoken 失败: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
		ErrCode     int    `json:"errcode"`
		ErrMsg      string `json:"errmsg"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("解析 gettoken 响应失败: %w", err)
	}
	if result.ErrCode != 0 {
		return "", fmt.Errorf("钉钉 gettoken 错误: %s", result.ErrMsg)
	}

	m.token = result.AccessToken
	m.expiresAt = time.Now().Add(time.Duration(result.ExpiresIn) * time.Second)
	return m.token, nil
}
