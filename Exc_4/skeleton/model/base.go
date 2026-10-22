package model

import (
	"time"

	"gorm.io/gorm"
)

// Base db models should not be directly exposed to web
// This is not best-practise implementation
type Base struct {
	ID        uint           `json:"id" gorm:"primarykey"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	DeletedAt gorm.DeletedAt `gorm:"index"`
}
