package platform

import (
	"context"
	"net/http"

	"github.com/linjiongxin/go-tiny-claw/internal/engine"
)

// Bot 定义了所有 IM 平台接入的统一契约。
// 新增平台时，只需实现该接口并在包内 init 中注册即可，main.go 零改动。
type Bot interface {
	// Name 返回平台标识，用于日志和路由区分
	Name() string

	// Enabled 检查当前平台的环境变量是否已配置
	Enabled() bool

	// Launch 启动平台。
	// 对于 HTTP 回调型平台（如飞书），在此注册 webhook 路由；
	// 对于长连接型平台（如钉钉 Stream），在此启动 goroutine。
	Launch(ctx context.Context, mux *http.ServeMux, eng *engine.AgentEngine) error
}

var registry []Bot

// Register 注册一个新的平台 Bot。应在各平台包的 init 中调用。
func Register(b Bot) {
	registry = append(registry, b)
}

// All 返回所有已注册的平台 Bot。
func All() []Bot {
	return registry
}
