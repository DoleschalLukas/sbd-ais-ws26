# Software Architecture for Big Data - Exercise 3

This exercise we're going to implement a REST server in Golang with Chi, that exposes the following routes:

- GET `/api/menu`
  - Returns a slice (array) of drinks
- GET `/api/order/all`
  - Returns a slice of orders
- GET `/api/order/totalled`
    - Returns a map of order ID + how many of these drinks have been ordered
- POST `/api/order`
  - Accepts a single order JSON and stores it in memory
- GET `/openapi/*`
    - Serves OpenAPI documentation and can be used for testing
- GET `/`
    - Serves a simple dashboard showing your orders

The server is reachable via port :3000, i.e. http://localhost:3000.
A scaffold is provided to get you started in the `skeleton/` folder.
You need to complete the webservice by resolving all comments including a `todo` keyword.

Once you've created your REST routes, execute the `build-openapi-docs.sh` script to create an OpenAPI compatible
REST documentation.

## Model Definitions
```
Drink
{
    description	string
    id	        integer
    name        string
    price       number
}
```

```
Order
{
    amount      integer
    created_at  string
    drink_id    integer // foreign key
}
```


