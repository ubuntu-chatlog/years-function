"use strict"
const axios = require("axios");
const deepmerge = require("deepmerge");
const elasticsearch_1 = require("elasticsearch");
const es = new elasticsearch_1.Client({
    host: process.env.ELASTICSEARCH_URL || "http://localhost:9200",
});
const apiGateway = process.env.API_GATEWAY || "http://gateway:8080/function";

const maybeGetChannelFromInput = content => {
    if (content && content.length) {
        const json = JSON.parse(content);
        if (json.channel && typeof json.channel === 'string') {
            return json.channel;
        }
    }
    return '';
}

const maybeGetAllChannels = _ => {
    return axios.get(`${apiGateway}/ubulog-channels`).then((data) => {
        if (data.data) {
            return data.data;
        }

        return [];
    });
}

const maybeGetSubQuery = async channelPromise => {
    const allChannelsPromise = maybeGetAllChannels();
    const allChannels = await allChannelsPromise,
          channel = await channelPromise;
    if (allChannels.includes(channel)) {
        return {
            "channels": {
                "filter": {
                    "term": {
                        "channel.keyword": channel,
                    },
                },
            },
        };
    }
    return false;
}

const getQuery = async subQueryPromise => {
    const query = {
        "body": {
            "size": 0,
            "aggs": {
                "years": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "interval": "year",
                    },
                },
            },
        },
    };
    
    const subQuery = await subQueryPromise;
    if (subQuery) {
        const q = { "body": { "size": 0, "aggs": {} } };
        q.body.aggs.channels = subQuery.channels;
        q.body.aggs.channels["aggs"] = query.body.aggs;
        return q;
    }

    return query;
}

const extractYears = aggs => {
    if (aggs.channels && aggs.channels.years) {
        return aggs.channels.years;
    }
    if (aggs.years) {
        return aggs.years;
    }
    return [];
}

module.exports = (content, callback) => {
    getQuery(maybeGetSubQuery(maybeGetChannelFromInput(content)))
    .then(query => {
        es.search(query).then(r => {
            callback(undefined, JSON.stringify(
                extractYears(r.aggregations).buckets
                .map(v => v.key_as_string)
                .map(v => v.substr(0, 4))
                .sort()));
        })
    })
    .catch(e => { callback(e, undefined); })
}
