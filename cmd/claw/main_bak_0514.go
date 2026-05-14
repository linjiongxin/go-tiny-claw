// cmd/claw/main.go
package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/linjiongxin/go-tiny-claw/internal/engine"
	"github.com/linjiongxin/go-tiny-claw/internal/platform"
	"github.com/linjiongxin/go-tiny-claw/internal/provider"
	"github.com/linjiongxin/go-tiny-claw/internal/tools"

	// 以下 import 仅用于触发各平台包的 init 注册
	_ "github.com/linjiongxin/go-tiny-claw/internal/dingtalk"
	_ "github.com/linjiongxin/go-tiny-claw/internal/feishu"
)

func main() {
	workDir, _ := os.Getwd()

	if os.Getenv("ZHIPU_API_KEY") == "" {
		log.Fatal("请先导出 ZHIPU_API_KEY 环境变量")
	}
	llmProvider := provider.NewZhipuOpenAIProvider("glm-4.5-air")

	registry := tools.NewRegistry()
	registry.Register(tools.NewReadFileTool(workDir))
	registry.Register(tools.NewWriteFileTool(workDir))
	registry.Register(tools.NewBashTool(workDir))
	registry.Register(tools.NewEditFileTool(workDir))

	// 开启慢思考
	eng := engine.NewAgentEngine(llmProvider, registry, workDir, true)

	// 统一启动所有已启用的平台 Bot
	mux := http.NewServeMux()
	ctx := context.Background()

	for _, bot := range platform.All() {
		if !bot.Enabled() {
			continue
		}
		if err := bot.Launch(ctx, mux, eng); err != nil {
			log.Printf("启动 %s 失败: %v", bot.Name(), err)
		} else {
			log.Printf("✅ %s 已启用", bot.Name())
		}
	}

	port := ":8899"
	log.Printf("🚀 go-tiny-claw 服务端已启动，正在监听 %s 端口\n", port)

	err := http.ListenAndServe(port, mux)
	if err != nil {
		log.Fatalf("服务器启动失败: %v", err)
	}
}
