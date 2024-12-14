package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/RhoNit/optimistic_locking_in_redis/config"
	"github.com/RhoNit/optimistic_locking_in_redis/model"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func createProduct(client *redis.Client, productId string, initialStock int) {
	client.HSet(ctx, productId, map[string]interface{}{
		"stock":   initialStock,
		"version": 1,
	})
}

func fetchProduct(client *redis.Client, productId string) *model.Product {
	stock1, err := client.HGet(ctx, productId, "stock").Result()
	if err != nil {
		fmt.Printf("Error while fetching the stock of: %q\n", err)
	}

	v1, err := client.HGet(ctx, productId, "version").Result()
	if err != nil {
		fmt.Printf("Error while fetching the version of: %q\n", err)
	}

	stock, _ := strconv.Atoi(stock1)
	version, _ := strconv.Atoi(v1)

	return &model.Product{
		ID:      productId,
		Stock:   stock,
		Version: version,
	}
}

func updateStock(client *redis.Client, product *model.Product, changeInStock int) error {
	txf := func(tx *redis.Tx) error {
		// fetch the current version inside the transaction
		productData, err := tx.HGetAll(ctx, product.ID).Result()
		if err != nil {
			return fmt.Errorf("failed to fetch product: %q", err)
		}

		currentStock, _ := strconv.Atoi(productData["stock"])
		currentVersion, _ := strconv.Atoi(productData["version"])

		if currentVersion != product.Version {
			return fmt.Errorf("version mismatch: requested %d, got %d\n", product.Version, currentVersion)
		}

		// update the stock.. and version as well
		stockAfterChange := currentStock + changeInStock
		if stockAfterChange < 0 {
			return fmt.Errorf("insufficient stock: requested a change of %d unit, but available stock: %d\n", changeInStock, currentStock)
		}

		_, err = tx.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HSet(ctx, product.ID, map[string]interface{}{
				"stock":   stockAfterChange,
				"version": currentVersion + 1,
			})
			return nil
		})

		return err
	}

	// execute the transaction
	for i := 0; i < 3; i++ {
		err := client.Watch(ctx, txf, product.ID)
		if err == nil {
			fmt.Printf("Product: %s is updated successfully.. new stock: %d units\n", product.ID, product.Stock+changeInStock)
			return nil
		}

		if err == redis.TxFailedErr {
			fmt.Printf("Transaction failed. Retrying...\n")
			time.Sleep(10 * time.Millisecond)
			continue
		}
	}

	return fmt.Errorf("failed to update product after retries")
}

func simulateConcurrentStockHandling(client *redis.Client, productId string) {
	// product accessed by the client #1
	product1 := fetchProduct(client, productId)

	// product accessed by the client #2
	product2 := fetchProduct(client, productId)

	// client 1 tries to update the stock value
	err := updateStock(client, product1, -5)
	if err != nil {
		fmt.Printf("Client 1 has failed to update stock: %q\n", err)
	}

	// client 2 is trying to update the stock value
	err = updateStock(client, product2, -10)
	if err != nil {
		fmt.Printf("Client 2 has failed to update stock: %q\n", err)
	}
}

func main() {
	// initialize redis connection
	rdb := config.InitCache()

	// store initial stock value of a product in the inventory
	productID := "product:917:stock"
	stock := 50
	createProduct(rdb, productID, stock)

	// simulate concurrency: by trying to access the same product by 2 clients
	simulateConcurrentStockHandling(rdb, productID)

}
