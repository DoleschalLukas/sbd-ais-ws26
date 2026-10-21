package model

type Drink struct {
	ID uint64 `json:"id"`
	// todo Add fields: Name, Price, Description with suitable types
	Name        string  `json:"name"`
	Price       float32 `json:"price"`
	Description string  `json:"description"`
	// todo json attributes need to be snakecase, i.e. name, created_at, my_variable, ..
}
