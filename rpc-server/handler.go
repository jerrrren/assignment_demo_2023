package main

import (
	"context"
	"strings"

	"github.com/TikTokTechImmersion/assignment_demo_2023/rpc-server/kitex_gen/rpc"
)

// IMServiceImpl implements the last service interface defined in the IDL.
type IMServiceImpl struct{}

func (s *IMServiceImpl) Send(ctx context.Context, req *rpc.SendRequest) (*rpc.SendResponse, error) {
	

	message := &Message{
		Content: req.Message.GetText(),
		Sender: req.Message.GetSender(),
		SendTime: req.Message.GetSendTime(),
	}

	chatKey := standardiseChatKey(req.Message.GetChat())

	err := redisClient.SaveMessage(ctx, chatKey, message)

	if(err != nil) {
		return nil, err	
	}

	resp := rpc.NewSendResponse()
    resp.Code, resp.Msg = 0, "success"

	return resp, nil
}

func (s *IMServiceImpl) Pull(ctx context.Context, req *rpc.PullRequest) (*rpc.PullResponse, error) {
	chatKey := standardiseChatKey(req.Message.GetChat())
	
	start := req.GetCursor()
	end := start + int64(req.GetLimit())

	messages,err := redisClient.getMessagesByChat(ctx, chatKey, start, end, req.GetReverse())
	if err != nil {
       return nil, err
    }

    var rpcMessages []*rpc.Message
    var counter int64 = 0
	var has_more = false
	var nextCursor = 0
	for _, message := range messages {

		if(counter +1 > req.GetLimit()) {
			has_more = true
			nextCursor = end
			break
		}
		rpcMessage := &rpc.Message{
			Chat: req.GetChat(),
			Sender: message.Sender(),
			Text: message.Content(),
			SendTime: message.SendTime(),
		}
		rpcMessages = append(rpcMessages, rpcMessage)
		counter += 1

	resp := rpc.NewPullResponse()
	resp.Code = 0
	resp.Msg = "success"
	resp.Messages = rpcMessages
	resp.HasMore = &has_more
	resp.NextCursor = &nextCursor

	return resp, nil
}



func standardiseChatKey(chatKey string) (string) {
	var standardisedChatKey string

	splitKey := strings.Split(chatKey,":")

	firstPart := splitKey[0]
	secondPart := splitKey[1]

	if(strings.Compare(firstPart,secondPart) < 0) {
		standardisedChatKey = firstPart + ":" + secondPart
	} else {	
		standardisedChatKey = secondPart + ":" + firstPart
	}

	return standardisedChatKey
}
