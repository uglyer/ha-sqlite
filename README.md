# HA-Sqlite

https://github.com/uglyer/ha-sqlite

## 说明

> 目前项目还处于早期开发阶段

## 目标

- 基于 ~~raft~~ 实现的 高可用的 sqlite 数据库服务

  - > 通过 raft 实现 wal 拷贝性能与预期出入较大, 考虑通过其他方式实现高可用
    > 
    > 启用 vfs wal in memory 连续插入每秒约 12000
    > 
    > 启用 raft wal replication 连续插入每秒约 35
    > 
    > 启用 raft sql statement replication 连续插入每秒约 270
    >
    > 以上结果在 apple m1 中执行

- 支持多节点多数据库创建与访问

- 支持事务操作、支持事务隔离 

- 支持每个库单独回快照、回滚操作

- 支持 S3 协议作为持久层

- 多操作系统支持: linux、windows、mac

- 跨平台支持: x86、arm


## 进度

- [x] Open
- [ ] Multi db (Developing)
- [x] Exec
- [x] Query
- [x] Driver
- [x] Prepare
- [x] Transaction isolation
- [ ] CI (Developing)
- [x] Wal copy
- [ ] Snapshot
- [ ] Restore
- [ ] S3
- [ ] `use` sql statement
- [ ] Trigger

## 为什么需要再造一个数据库的轮子

### 契机

工作中现有的业务背景引发的思考，在重前端复杂业务场景(如:在线协作的原型设计工具figma)下，
我可能需要一个轮子用于 降低前后端对接成本、更容易的实现基于状态驱动、更容易实现多人协作

### 业务背景

一个类似三维数据管理的saas平台，任何人都能创建项目加入三维数据(点云、网格、全景、绘制)，经过对齐堆叠后放在地图上(可选)，这个过程与很多GIS相关的项目很像。
然后进行一系列针对三维数据的编辑操作，数据裁剪/绘制仿真模型/创建标注热点/编辑故事版视图/编辑漫游路径/导出报表/渲染输出视频/作为大屏背景 等一系列操作

我们已经实现了上述所有功能，基于 webgl + react 实现的状态驱动编辑器，但是在Web前端保留的状态太多了而且都需要跟后端频繁交互。

举个例子：一个标注点存在三个元素(ICON、文本、引线), 可修改属性达100+(引线颜色、高度、起始点位置、文本字体、文本大小、文本居中方式、文本区域背景、文本描边属性、ICON地址、ICON尺寸、是否需要开启遮挡、是否使用近大远小、启用避免文本ICON区域重叠 等)
当然，一般情况下，大部分属性都是使用默认值并且不会发生变化，但是在产品设计上，依旧允许调整这么多的属性。
在前端渲染的属性面板中，用户拖动活动条导致一个属性发生变化时，webgl 中渲染的内容需要同步发生变化，而且前端渲染的属性面板可能不止一个，是的，他们都要同步发生变化。
（这个过程像blender这样的软件，一个大窗口内能创建多个小窗口，但是都是在维护同一个项目）
现在我们基于一个自己实现的前端Store状态分发实现, 支持复杂的JSON树结构(中间可能嵌套多层数组)，能够校验到最小节点的变化事件然后应用至 webgl 中。
麻烦的是，属性修改后，用户如果点击取消，需要回滚状态。但如果点击确定或者直接关闭，需要将变化的内容调用接口增量更新至后端。
后端定义的数据结构通常是为了更好的存储，这往往与前端定义的结构不一致，前端需要筛选哪些属性发生变化并将他转换为后端的结构。

按照这个描述实现，会有几个显而易见的问题：
- 为了提高数据编辑的效率，同一个项目不同的人都打开进行操作，如果不小心操作了相同的数据，这是个灾难。
- 后端把数据分散的存放在各个表中，每个更新动作为真实更新，如果有错误操作想要回滚到某个历史版本或者基于历史版本创建一个新的项目会非常麻烦。
关于第一点，你可能会很快会想到创建一个 websocket 链接，来推送其他用户更新的数据。这个过程比较像figma或者在线协作文档等与后端通讯的过程。
后端把数据分散的存放在各个表中，现有的接口调用方式需要去监听一堆表数据的变化非常麻烦，而且实现快照回滚等功能也同样繁琐。

### 比较理想化的交互方式

前端已经需要维护一个 Store, 那如果我们把 Store 的实现调整为 基于 Sqlite，后端实现调整为一个项目即为一个 Sqlite db。
sql.js 支持内存数据库及触发器能够监听到属性变化可用于前端 Store 底层实现，当监听到内容变化后自动同步至 webgl / 属性面板 / 后端
后端部分单库作为文件存储，同样可快速实现快照等功能，并且支持以S3协议作为数据的持久层。
数据结构通过 json schema 约束。
- 在只读模式下，前端只需要加载 db 文件即可恢复 Store 状态，无后端接口压力。如果 db 文件体积过大, 还可通过 http range 请求按需增量加载。 
- 在编辑模式下，通过触发器自动完成对后端数据的更新，也同样依赖触发器，后端可将变化的内容主动推送至前端。

sqlite 为库级锁，通常单场景的同时写入并发不会太大，一个场景的数据体积总量有限，不会成为 sqlite 增删改查的性能瓶颈。

当然，这样相当于一个场景下的所有数据内容都暴露给前端，有整个项目编辑或项目读取权限的用户操作这似乎没有任何问题。
但不能排除产品提了一个想要精细化控制访问权限的需求，这部分我后续会提到。

### 为什么基于sqlite

- sqlite足够轻量且高性能
- 前端可以基于sqlite实现状态管理 [StoreLite.js](https://github.com/uglyer/storelite.js) (类似 redux/mobx.js)

### 有什么好处

- 减少后端性能压力，只读模式压力交给CDN
- 快速实现快照、回滚、分叉、导入导出 等功能
- 前后端对接不再需要为接口产生争执，只要约定 json schema (应该比graphql还轻松)
- 前端Store, 提供钩子快速绑定至组件状态，表单类业务结合 formily 等第三方库 可直接封装表单组件
- 快速实现多用户协作
- 降低 重前端交互场景下 通过事件传递消息的心智负担 (如: 事件循环依赖, 事件机制与组件状态绑定繁琐 等)

### 有哪些情况适合使用

- 需要在线协作的场景
  - 文档编辑
  - 在线的原型设计工具
  - 共享画板
- 大量基于状态/事件驱动的场景
  - 在线的原型设计工具
  - GIS/BIM/三维可视化编辑器
- 其他读多少写的场景

### 现成的轮子

高可用的 sqlite 有两个现成基于 raft 实现的库：[dqlite](https://github.com/canonical/go-dqlite), [rqlite](https://github.com/rqlite/rqlite)

他们写的很好，但是都各自存在一些问题不能满足我的使用

如：
- rqlite 不支持多数据库创建访问、不提供驱动仅提供http接口、基于sql语句的复制不能保证执行结果的一致性(`insert into foo values (random())`)
- dqlite 不支持 windows 平台
- 两者均不支持触发器、不支持事务隔离

### 结论

所以，这个轮子就诞生了
