package com.it.jxnu.kafka.producer;

/**
 * Created by Administrator on 2015-07-07.
 */
public class ProducerConfig {
    private String metadata_broker_list;
    private String request_required_acks;
    private String producer_type;
    private String serializer_class;


    public String getMetadata_broker_list() {
        return metadata_broker_list;
    }

    public void setMetadata_broker_list(String metadata_broker_list) {
        this.metadata_broker_list = metadata_broker_list;
    }

    public String getRequest_required_acks() {
        return request_required_acks;
    }

    public void setRequest_required_acks(String request_required_acks) {
        this.request_required_acks = request_required_acks;
    }

    public String getProducer_type() {
        return producer_type;
    }

    public void setProducer_type(String producer_type) {
        this.producer_type = producer_type;
    }

    public String getSerializer_class() {
        return serializer_class;
    }

    public void setSerializer_class(String serializer_class) {
        this.serializer_class = serializer_class;
    }
}
