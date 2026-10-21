package repository

import (
	"ordersystem/model"
	"time"
)

type DatabaseHandler struct {
	// drinks represent all available drinks
	drinks []model.Drink
	// orders serves as order history
	orders []model.Order
}

// todo
func NewDatabaseHandler() *DatabaseHandler {
	// Init the drinks slice with some test data
	// drinks := ...
	drinks := []model.Drink{
		{ID: 1, Name: "Espresso", Price: 2.50, Description: "Coffee"},
		{ID: 2, Name: "Beer", Price: 4.50, Description: "Alcoholic Beverage"},
		{ID: 3, Name: "Coca Cola", Price: 2.00, Description: "Popular American Soda"},
		{ID: 4, Name: "Pepsi", Price: 2.00, Description: "Coca Cola but worse"},
	}

	// Init orders slice with some test data
	orders := []model.Order{
		{DrinkID: 1, CreatedAt: time.Now(), Amount: 3},
		{DrinkID: 2, CreatedAt: time.Now(), Amount: 4},
		{DrinkID: 1, CreatedAt: time.Now(), Amount: 3},
		{DrinkID: 4, CreatedAt: time.Now(), Amount: 1},
	}
	return &DatabaseHandler{
		drinks: drinks,
		orders: orders,
	}
}

func (db *DatabaseHandler) GetDrinks() []model.Drink {
	return db.drinks
}

func (db *DatabaseHandler) GetOrders() []model.Order {
	return db.orders
}

// todo
func (db *DatabaseHandler) GetTotalledOrders() map[uint64]uint64 {
	// calculate total orders
	// key = DrinkID, value = Amount of orders
	// totalledOrders map[uint64]uint64

	totalledOrders := make(map[uint64]uint64)

	orders := db.GetOrders()

	for _, order := range orders {
		totalledOrders[order.DrinkID] += order.Amount
	}

	return totalledOrders
}

func (db *DatabaseHandler) AddOrder(order *model.Order) {
	// todo
	// add order to db.orders slice
	db.orders = append(db.orders, *order)
}
