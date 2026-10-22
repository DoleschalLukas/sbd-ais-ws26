package model

type Drink struct {
	Base
	Name        string  `json:"name" gorm:"unique,not null"`
	Price       float32 `json:"price"`
	Description string  `json:"description"`
}
