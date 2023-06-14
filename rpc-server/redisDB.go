package main

import (
	"context"
	"encoding/json"

	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	client *redis.Client
}

type Message struct {
	Sender    string `json:"sender"`
	Content   string `json:"message"`
	SendTime 	int64  `json:"timestamp"`
}

func  NewRedisClient() *RedisClient {
	return &RedisClient{
		client: redis.NewClient(&redis.Options{
			Addr:     "redis:6379", // Replace with your Redis server address
			Password: "",               // Replace with your Redis password
			DB:       0,                // Replace with your Redis database number
		}),
	}
}

func (c *RedisClient) SaveMessage(ctx context.Context, chat string, message *Message) error {
	// Store the message in json
	jsonData, err := json.Marshal(message)
	if err != nil {
		return err
	}

	sorted_set_member := &redis.Z{
		Score:  float64(message.SendTime),
		Member: jsonData,
	}
	_ , err = c.client.ZAdd(ctx, chat, *sorted_set_member).Result()
    if err != nil {
        return err
    }

	return nil
}

func (c *RedisClient) GetMessagesByChat(ctx context.Context, chat string, start int64, end int64, forward bool) ([]*Message, error) {
	// Get the messages with room ID
	var messages []*Message
	var err error
	var dataSlice []string


	if forward {
		dataSlice, err = c.client.ZRange(ctx, chat, start, end).Result()
		if err != nil {
			return nil, err
		}

	} else {
		dataSlice, err = c.client.ZRevRange(ctx, chat, start, end).Result()
		if err != nil {
			return nil, err
		}
	}

	for _, member := range dataSlice {
    	var message Message

		err := json.Unmarshal([]byte(member), &message)
		if err != nil {
			return nil,err
		} 
		messages = append(messages,&message) 
	}

	return messages, nil
}