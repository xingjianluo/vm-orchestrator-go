package orchestrator

import (
	"github.com/xingjianluo/vm-orchestrator-go/internal/model"
	"github.com/xingjianluo/vm-orchestrator-go/internal/quota"
)

// newPoolForRebuild 封装配额池构造，供对账重算使用。
func newPoolForRebuild(total model.Resources) *quota.Pool { return quota.New(total) }
