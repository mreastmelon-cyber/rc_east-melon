#!/usr/bin/env python3
"""Render recorded RPC/log/SQL evidence for real browser screenshots.

This renderer never calls the service and never invents response data.
"""
import argparse
import html
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    root = args.directory
    def data(name):
        return json.loads((root / name).read_text())
    def esc(value):
        return html.escape(str(value))
    def block(title, text):
        return f'<section><h2>{esc(title)}</h2><pre>{esc(text)}</pre></section>'
    def code(title, value):
        return block(title, json.dumps(value, ensure_ascii=False, indent=2))
    def table(title, pairs):
        rows = ''.join(f'<tr><td>{esc(k)}</td><td><code>{esc(v)}</code></td></tr>' for k,v in pairs)
        return f'<section><h2>{esc(title)}</h2><table>{rows}</table></section>'
    m = data('manifest.json')
    health = data('01-health.json')
    timing = data('02-rpc-timing.json')
    db = data('04-database.json')
    task = data('04-task.json')
    attempts = data('04-attempts.json')
    duplicate = data('05-duplicate-response.json')
    accepted = data('02-submit-response.json')
    states = data('04-observed-states.json')
    compose = [json.loads(line) for line in (root/'01-compose-status.txt').read_text().splitlines() if line.strip()]
    titles = ['服务已启动并就绪', 'gRPC 提交新购买事件', 'Worker 调用库存模拟供应商', '成功落库与重复请求验证']
    files = ['01-ready.html', '02-submit.html', '03-delivery.html', '04-result.html']
    css = '''*{box-sizing:border-box}body{margin:0;background:#f1f5f9;color:#172537;font:15px/1.5 -apple-system,BlinkMacSystemFont,"PingFang SC",Arial,sans-serif}main{max-width:1250px;margin:auto;padding:26px 30px}header{margin-bottom:20px}.eyebrow{color:#51657a;font-size:13px}h1{margin:8px 0;font-size:28px}h2{font-size:16px;margin:0 0 12px}p{margin:9px 0;color:#41566e}.id{font-size:13px;overflow-wrap:anywhere}nav{display:flex;gap:8px;margin:18px 0;flex-wrap:wrap}nav a{color:#345579;text-decoration:none;padding:8px 12px;background:white;border:1px solid #d5dfeb;border-radius:7px}nav a.active{color:white;background:#275db3;border-color:#275db3}.cols{display:grid;grid-template-columns:1fr 1fr;gap:16px}section{background:white;border:1px solid #d9e2ed;border-radius:9px;margin-bottom:16px;padding:18px;min-width:0}pre{background:#122238;color:#e7f0fb;border-radius:7px;padding:16px;white-space:pre-wrap;overflow-wrap:anywhere;margin:0;font:13px/1.6 ui-monospace,SFMono-Regular,Consolas,monospace}code{font:13px/1.55 ui-monospace,SFMono-Regular,Consolas,monospace;overflow-wrap:anywhere}table{border-collapse:collapse;width:100%;font-size:14px}td,th{text-align:left;padding:9px 10px;border-bottom:1px solid #e6edf5;vertical-align:top}td:first-child{width:37%;color:#50637a}.badge{display:inline-block;background:#e1f3e9;color:#176847;border:1px solid #b9dfcb;font-weight:600;padding:5px 12px;border-radius:6px}.strip{border-left:4px solid #279169;background:#e9f7ef;padding:12px 16px;border-radius:5px;margin:12px 0 18px}.flow{font-size:16px;color:#214778;margin:10px 0}footer{font-size:12px;color:#63778d;padding:4px 0 15px}@media(max-width:800px){.cols{grid-template-columns:1fr}main{padding:20px}}'''
    pages = []
    rows = [(x['Service'], x['State'] + (' / '+x['Health'] if x.get('Health') else '')) for x in compose]
    pages.append('<div class="strip">健康检查原始返回：<strong>'+esc(health['status'])+'</strong>。容器状态来自 docker compose ps。</div>'+table('实际容器状态',rows)+code('gRPC Health 检查结果 · 01-health.json',health)+table('测试服务访问方式',[('gRPC',health['grpc_address']),('MySQL','127.0.0.1:33068 / notification'),('Prometheus','http://127.0.0.1:9090/metrics'),('模拟供应商','Compose 内部 mock-provider:8081')]))
    pages.append('<p class="flow">客户端 → gRPC 认证与校验 → MySQL 事务保存事件与任务</p><div class="cols">'+code('实际请求 · 02-submit-request.json',data('02-submit-request.json'))+code('SubmitEvent 原始响应 · 02-submit-response.json',accepted)+'</div>'+code('实际调用时间与状态 · 02-rpc-timing.json',timing)+'<div class="strip">accepted='+esc(accepted['accepted'])+'，duplicate='+esc(accepted['duplicate'])+'：本次是新事件，接入事务已提交。最终投递结果在第 4 步核实。</div>')
    pages.append('<p class="flow">MySQL 任务 → Worker → HTTP POST /inventory → 模拟供应商 HTTP 200</p>'+table('数据库中的请求快照',[(k,db[k]) for k in ['task_id','destination_id','request_method','request_url','idempotency_key','body_sha256']])+code('实际供应商请求 Body',db['request_body'])+block('模拟供应商实际接收日志 · 03-provider.log',(root/'03-provider.log').read_text())+block('Worker 实际完成日志 · 03-worker.log',(root/'03-worker.log').read_text())+'<div class="strip">接收日志与数据库使用同一幂等键和 body_sha256；Worker 日志使用同一 task_id。</div>')
    pages.append('<div class="strip"><strong>最终任务状态：'+esc(task['state'])+'</strong> · HTTP '+esc(db['last_http_status'])+' · 尝试次数 '+esc(task['attemptCount'])+'</div>'+table('MySQL 实际落库查询 · 重复提交后',[(k,db[k]) for k in ['source_event_id','event_id','task_id','state','last_http_status','event_rows','task_rows','attempt_rows','completed_at']])+'<div class="cols">'+code('ListAttempts 原始响应 · 04-attempts.json',attempts)+code('重复提交原始响应 · 05-duplicate-response.json',duplicate)+'</div>'+code('实际观察到的任务状态',states))
    for i,body in enumerate(pages):
        nav=''.join(f'<a class="{"active" if i==j else ""}" href="{name}">{j+1}. {esc(titles[j])}</a>' for j,name in enumerate(files))
        header=f'<header><div class="eyebrow">外部 API 通知服务 · 本地实际请求验证 · {esc(m["run_started_at"])}</div><h1>{i+1}. {esc(titles[i])}</h1><div class="id">事件 <code>{esc(m["source_event_id"])}</code> · event_id={m["event_id"]} · task_id={m["task_id"]}</div><nav>{nav}</nav></header>'
        document=f'<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{esc(titles[i])}</title><style>{css}</style><main>{header}{body}<footer>浏览器展示本次真实采集的 RPC、日志和数据库记录；原始文件均保存在当前目录。此页面用于截图说明，不是项目管理前端。凭据未展示。</footer></main></html>'
        (root/files[i]).write_text(document)
    print('Rendered 4 evidence pages from recorded data:',root)

if __name__ == '__main__':
    main()
