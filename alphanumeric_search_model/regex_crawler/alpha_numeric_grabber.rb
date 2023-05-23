# Query ElasticSearch for RegEx Patterns with scroll
#   Need to do a Point In Time with keep alive, this won't be needed if we use a dedicated cluster
# Extract RegEx Patterns from each Doc
# Create list of all RegEx Patterns
# Create Unique list of regex patterns

require 'httparty'
require 'json'

@query = {
    :sort => [
        {
            :_score => {
                :order => "desc"
            }
        }
    ],
    :size => 5000,
    :query => {
        :bool => {
            :filter => {
                :regex => { }
            }
        }
    # },
    # :_source => {
    #     :includes => [
    #         "extension",
    #         "created",
    #         "created_at",
    #         "description",
    #         "language",
    #         "title",
    #         "content",
    #         "tags",
    #         "path",
    #         "promote",
    #         "domain_name",
    #         "basename",
    #         "updated_at",
    #         "title_en",
    #         "updated",
    #         "changed",
    #         "url_path",
    #         "click_count",
    #         "mime_type",
    #         "audience",
    #         "searchgov_custom2",
    #         "searchgov_custom3",
    #         "searchgov_custom1",
    #         "content_type",
    #         "thumbnail_url"
    #     ]
    }
}

# @es_fields = [
#     "title_en.raw",
#     "title_en.raw.keyword",
#     # "content_en.raw",
#     # "content_en.raw.keyword"
# ]

@indices = {
    "production-i14y-documents-searchgov-v6-reindex_keyword" => [
        "title_en.raw",
        "title_en.raw.keyword",
        "content_en.raw",
        "content_en.raw.keyword"
    ],
    "human-logstash-*" => [
        "params.query.raw"
    ]
}

@es_url = [
    # "http://es717x1:9200",
    "http://es717x2:9200",
    "http://es717x3:9200",
    "http://es717x4:9200",
]

@es_node = 0

regex_expressions = [
    '[0-9]+[a-z]+',
    "[a-z]+[0-9]+",
    "([a-z]+[0-9]*-[a-z]+[0-9]*)",
    "([a-z]+[0-9]*-[0-9]+[a-z]*)",
    "([0-9]+[a-z]*-[a-z]+[0-9]*)",
    "([0-9]+[a-z]*-[0-9]+[a-z]*)",
    "([a-z]+[0-9]*§[a-z]+[0-9]*)",
    "([a-z]+[0-9]*§[0-9]+[a-z]*)",
    "([0-9]+[a-z]*§[a-z]+[0-9]*)",
    "([0-9]+[a-z]*§[0-9]+[a-z]*)",
    "([a-z]+[0-9]*.[a-z]+[0-9]*)",
    "([a-z]+[0-9]*.[0-9]+[a-z]*)",
    "([0-9]+[a-z]*.[a-z]+[0-9]*)",
    "([0-9]+[a-z]*.[0-9]+[a-z]*)"
]

def query_elasticsearch(url, index, request)
    # puts JSON.pretty_generate(request)
    return JSON.parse(HTTParty.post(
        "#{url}/_search/scroll",
        :body => JSON.generate(request),
        :headers => {'Content-Type' => 'application/json'}
    ).to_s)
end

def create_scroll_elasticsearch(url, index, request)
    # puts JSON.pretty_generate(request)
    return JSON.parse(HTTParty.post(
        "#{url}/#{index}/_search?scroll=10m",
        :body => JSON.generate(request),
        :headers => {'Content-Type' => 'application/json'}
    ).to_s)
end


def push_to_elasticsearch(url, index, body)
    es_payload = []
    body.each do |doc|
        es_payload.push(JSON.generate({:index => {:_index => index, :_type => "_doc"}}))
        es_payload.push(JSON.generate(doc))
    end
    # puts (es_payload.join("\n") + "\n")
    HTTParty.post(
        "#{url}/#{index}/_bulk",
        :body => (es_payload.join("\n") + "\n"),
        :headers => {'Content-Type' => 'application/json'}
    )
end

def create_i14y_doc(document, regex, field)
    return {
        :domain_name => document["_source"]["domain_name"],
        :regex_patterns => document["_source"][field].scan(Regexp.new(regex)).flatten
    }
end

def create_logstash_doc(document, regex, field)
    scan_field = field.gsub("params.", "")
    # puts scan_field
    # puts document["_source"]["params"][scan_field]
    return {
        :affiliate => document["_source"]["params"]["affiliate"],
        :regex_patterns => document["_source"]["params"][scan_field].scan(Regexp.new(regex)).flatten
    }
end

def crawl_es_index_with_field(es_url, index, field, regex)
    puts "*****************   REGEX PATTERN: #{regex} ******************"
    results = create_scroll_elasticsearch(es_url[@es_node], index, @query.merge({:query => {:bool => {:filter => [{:regexp => {field => regex}}]}}}))
    scroll_id = results["_scroll_id"]
    puts scroll_id
    puts "Doc Count: " + results["hits"]["total"]["value"].to_s + " Relation: " + results["hits"]["total"]["relation"].to_s

    # exit 0
    num_docs = 0
    # es_node = 0
    temp_doc_list = []
    loop do
        puts es_url.at(@es_node)
        # results = query_elasticsearch(es_url, index,
        #     @query.merge({:query => {:bool => {:filter => [{:regexp => {field => regex}}]}}}).merge(
        #         {:from => num_docs}).merge({
        #             :pit => {
        #                 :id => pit_id,
        #                 :keep_alive => "60m"
        #             }
        #         })
        # )

        # exit 0

        # puts JSON.pretty_generate(results)

        # exit 0
        if results["hits"]["hits"].size == 0
            break
        end
        
        num_docs = num_docs + results["hits"]["hits"].size

        results["hits"]["hits"].each do |document|
            # puts JSON.pretty_generate(document)
            scan_field = field.gsub(".raw", "").gsub(".keyword", "")
            temp_doc_list.push(index.include?("i14y") ? create_i14y_doc(document, regex, scan_field) : create_logstash_doc(document, regex, scan_field))
            # Write results back to ES
        end
        push_to_elasticsearch(es_url[@es_node], index + "_regex", temp_doc_list)
        temp_doc_list = []

        results = query_elasticsearch(es_url.at(@es_node), index,
            {
                :scroll => "10m",
                :scroll_id => scroll_id
            }
        )
        @es_node = (@es_node + 1) % es_url.size
    end
    # Write results back to ES
end

@indices.keys.each do |index|
    @indices[index].each do |field|
        regex_expressions.each do |regex|
            begin
                crawl_es_index_with_field(@es_url, index, field, regex)
            rescue => exception
                puts exception.backtrace
                puts "There was an error"
                sleep(120)
            end
        end
    end
end

# @es_fields.each do |field|
#     regex_expressions.each do |regex|
#         puts field + " " + regex
#         num_docs = 0
#         loop do
#             results = query_elasticsearch(
#                 "http://localhost:9200", 
#                 "production-i14y-documents-searchgov-v6-reindex_keyword",
#                 # query[:query][:bool][:filter][:regex] = {field => regex}
#                 query.merge({:query => {:bool => {:filter => [{:regexp => {field => regex}}]}}}).merge(
#                     {:from => num_docs})
#             )
#             puts "Total/Count: " + results["hits"]["total"]["value"].to_s + " " +
#                 results["hits"]["hits"].size.to_s + " " + num_docs.to_s
#             # puts JSON.pretty_generate(results)
#             if results["hits"]["hits"].size == 0 or num_docs == 10
#                 break
#             end
            
#             # Extract regex patterns from the field
#             results["hits"]["hits"].each do |document|
#                 scan_field = field.gsub(".raw", "").gsub(".keyword", "")
#                 puts scan_field + ": " + document["_source"][scan_field]
#                 puts Regexp.new(regex.gsub(".", "\\."))#/[0-9]+[a-z]+/
#                 puts document["_source"][scan_field].scan(Regexp.new(regex.gsub(".", "\."))).join(",")

#             end

#             num_docs = num_docs + results["hits"]["hits"].size
#         end
#     end
# end