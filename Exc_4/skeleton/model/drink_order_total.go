package model

// Webmodel DO NOT USE IN DB
type DrinkOrderTotal struct {
	DrinkID            uint64 `json:"drink_id" gorm:"column:drink_id"`
	TotalAmountOrdered uint64 `json:"total_amount_ordered" gorm:"column:total_amount_ordered"`
}
