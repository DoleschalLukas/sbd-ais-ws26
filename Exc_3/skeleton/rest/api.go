package rest

import (
	"encoding/json"
	"net/http"
	"ordersystem/model"
	"ordersystem/repository"

	"github.com/go-chi/render"
)

// GetMenu 			godoc
// @tags 			Menu
// @Description 	Returns the menu of all drinks
// @Produce  		json
// @Success 		200 {array} model.Drink
// @Router 			/api/menu [get]
func GetMenu(db *repository.DatabaseHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// todo
		// get slice from db
		orders := db.GetOrders()
		render.Status(r, http.StatusOK)
		render.JSON(w, r, orders)
	}
}

// todo create GetOrders /api/order/all
func GetOrdersTotal(db *repository.DatabaseHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Call method we created earlier to get map of totals
		totals := db.GetTotalledOrders()

		render.Status(r, http.StatusOK)
		render.JSON(w, r, totals)
	}
}

func GetOrders(db *repository.DatabaseHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Fetch orders from database
		orders := db.GetOrders()

		// Return slice of orders
		render.Status(r, http.StatusOK)
		render.JSON(w, r, orders)
	}
}

// todo create GetOrdersTotal /api/order/total

// PostOrder 		godoc
// @tags 			Order
// @Description 	Adds an order to the db
// @Accept 			json
// @Param 			b body model.Order true "Order"
// @Produce  		json
// @Success 		200
// @Failure     	400
// @Router 			/api/order [post]
func PostOrder(db *repository.DatabaseHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var newOrder model.Order
		err := json.NewDecoder(r.Body).Decode(&newOrder)
		if err != nil {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, map[string]string{"error": "Invalid request payload"})
			return
		}
		db.AddOrder(&newOrder)

		// Render success response
		render.Status(r, http.StatusOK)
		render.JSON(w, r, "ok")
	}
}
