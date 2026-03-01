# 任务看板 — Daft
> Tech Lead 写入 | Teammate 执行

## 状态：⏳ 待启动 | 🔄 进行中 | 🔍 待巡检 | ✅ 通过 | ❌ 打回

---

## 当前任务

### TASK-D-001：M0 环境验证与架构摸底
**状态**：⏳ 待启动  |  **优先级**：🔴 高（阻塞所有后续任务）

**验收标准**：
- [ ] 开发环境可用（编译/导入命令 + 完整原始日志）
- [ ] 现有测试套件全通过（提供完整输出）
- [ ] ARCH_NOTES.md 内容完整（见下方要求）
- [ ] Discussion.md 中发起架构方案讨论并等待 Tech Lead 确认
- [ ] 在 Daft/ submodule 内完成 commit，通知 Tech Lead sync

**ARCH_NOTES.md 要求**：
- Runner 扩展点分析（Ray/Python/Native）
- LogicalPlan / PhysicalPlan 关键节点
- Expression 系统扩展机制
- DuckDB 接入方案（至少两种）及优劣对比
- 推荐方案及理由

**预期输出物**：
- `Daft/ARCH_NOTES.md`
- `Daft/CHANGES.md` 更新
- Daft/ submodule 内的 git commit

---

## 历史任务
（暂无）
