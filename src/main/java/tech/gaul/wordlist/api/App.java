package tech.gaul.wordlist.api;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClient;
import software.amazon.awssdk.services.cognitoidentityprovider.model.GetUserRequest;
import software.amazon.awssdk.services.cognitoidentityprovider.model.GetUserResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import tech.gaul.wordlist.api.models.SourceEntity;
import tech.gaul.wordlist.api.models.UpdateFromSourceMessage;

import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Lambda function entry point. You can change to use other pojo type or
 * implement
 * a different RequestHandler.
 *
 * @see <a
 *      href=https://docs.aws.amazon.com/lambda/latest/dg/java-handler.html>Lambda
 *      Java Handler</a> for more information
 */
public class App implements RequestHandler<APIGatewayProxyRequestEvent, Object> {
    private final SqsClient sqsClient = DependencyFactory.sqsClient();
    private final DynamoDbEnhancedClient dynamoDbClient = DependencyFactory.dynamoDbClient();
    private final DynamoDbTable<SourceEntity> sourcesTable = dynamoDbClient.table(System.getenv("SOURCES_TABLE_NAME"),
            TableSchema.fromBean(SourceEntity.class));
    private final CognitoIdentityProviderClient cognitoClient = DependencyFactory.cognitoIdentityProviderClient();

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent input, Context context) {
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();

        // Validate token
        String token = input.getHeaders().get("Authorization");
        if (token == null || !token.startsWith("Bearer ")) {
            return response.withStatusCode(401).withBody("{\"error\": \"Unauthorized\"}");
        }
        token = token.substring(7)
        GetUserResponse getUserResponse = cognitoClient.getUser(b -> b.accessToken(token));
        if (getUserResponse.username() == null) {
            return response.withStatusCode(401).withBody("{\"error\": \"Unauthorized\"}");
        }

        // Check that source exists
        String sourceId = input.getPathParameters().get("id");

        SourceEntity source = sourcesTable.getItem(Key.builder().partitionValue(sourceId).build());

        if (source == null) {
            return response
                    .withStatusCode(404)
                    .withBody("{\"message\": \"Source not found\"}");
        }

        // Send message to queue
        UpdateFromSourceMessage message = message.builder()
                .id(sourceId)
                .force(false)
                .build();

        SendMessageResponse sendMessageResponse = sqsClient.sendMessage(b -> b.queueUrl(System.getenv("UPDATE_FROM_SOURCE_QUEUE_URL")));

        if (!sendMessageResponse.sdkHttpResponse().isSuccessful()) {
            return response
            .withStatusCode(500)
            .withBody("{\"message\": \"Failed to request update for source " + sourceId + "\"}");
        }

        return response
                .withStatusCode(200)
                .withBody("{\"message\": \"Update requested for source " + sourceId + "\"}");
    }
}
