package tech.gaul.wordlist.api.models;

import lombok.Getter;
import lombok.Setter;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;

@DynamoDbBean
@Getter
@Setter
public class SourceEntity {
    private String id;
    private String name;
    private String url;

    @DynamoDbPartitionKey
    public String getId() {
        return id;
    }
}
