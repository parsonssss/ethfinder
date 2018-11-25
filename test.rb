require 'google/cloud/bigquery'

class Queryeth

    def initialize something_you_want_to_query=nil
        @@PROJECT_ID = 'ethfinder-223516'
        @bq = Google::Cloud::Bigquery.new project_id: @@PROJECT_ID
        @fields = something_you_want_to_query
    end
end



class Queryeth_contact < Queryeth
    def initialize contact_address
        super()
        @contact_address = contact_address
        @sql_query = "select input  from `bigquery-public-data.ethereum_blockchain.transactions` where to_address = \"#{@contact_address}\""
        @input = []
    end

    def get_contact_input
        _result = @bq.query @sql_query
        _result.each do |row|
            @input << row[:input]
        end
        @input
    end
            
end

class Queryeth_token < Queryeth
    def initialize token_address
        super()
        @token_address = token_address
        @holder_hash = Hash.new
        @@div_number = 1000000000000000000
    end

    def get_token_total_supply
        _sql_query = "SELECT total_supply,symbol FROM `bigquery-public-data.ethereum_blockchain.tokens` WHERE address= \"#{@token_address}\""
        _result = @bq.query _sql_query
        _result.each do |row|
            @total_supply = row[:total_supply].to_f / @@div_number
            @token_symbol = row[:symbol]
        end
        return @total_supply,@token_symbol
    end

    def get_token_holders
        _sql_query = "SELECT from_address, to_address FROM `bigquery-public-data.ethereum_blockchain.token_transfers` WHERE token_address= \"#{@token_address}\""
        _result = @bq.query  _sql_query
        @all_addr = []
        _result.each do |row|
            @all_addr << row[:from_address]
            @all_addr << row[:to_address]
        end

        @all_addr.uniq!
    end

    def get_holder_input holder_addr
        @all_input = 0
        _sql_query = "SELECT value FROM `bigquery-public-data.ethereum_blockchain.token_transfers` WHERE token_address = \"#{@token_address}\" and to_address = \"#{holder_addr}\""
        _result = @bq.query _sql_query
        _result.each do |row|
            @all_input += row[:value].to_f / @@div_number
        end
        @all_input
    end

    def get_holder_output holder_addr
        @all_output = 0
        _sql_query = "SELECT value FROM `bigquery-public-data.ethereum_blockchain.token_transfers` WHERE token_address = \"#{@token_address}\" and from_address = \"#{holder_addr}\""
        _result = @bq.query _sql_query
        _result.each do |row|
            @all_output += row[:value].to_f / @@div_number
        end
        @all_output
    end

    def get_holder_banlance holder_addr
        _all_input = get_holder_input holder_addr
        _all_output = get_holder_output holder_addr
        @holder_banlance = _all_input - _all_output
        @holder_banlance
    end

    def get_token_holder_rank
        _all_holdes = get_token_holders
        _total_supply,_token_symbol = get_token_total_supply
        _file_name = "#{_token_symbol}_holder_rank.csv"
        _a_hash = Hash.new
        _all_holdes.each do |holder_addr|
            _holder_blance = get_holder_banlance holder_addr
            puts "#{holder_addr} : #{_holder_blance}"
            _a_hash.store(holder_addr,_holder_blance)
        end
        @holder_hash = _a_hash.sort_by {|key,value| value}.to_h
        hash_write_to_file _file_name,@holder_hash
    end

    def hash_write_to_file filename,what_you_want_to_write
        f = File.new(filename,'w')
        what_you_want_to_write.each_pair do |key,value|
            line = "#{key} : #{value}"
            puts "now is writing #{line}"
            f.write(line + "\n")
        end
        f.close
    end

end


class Queryeth_block < Queryeth

    def initialize block_number,something_you_want_to_query=nil
        super(something_you_want_to_query)
        @block_number = block_number
        @sql_query_1 = "select #{@fields}  from `bigquery-public-data.ethereum_blockchain.blocks` where number = #{@block_number}"
        @sql_query_2 = "select transaction_hash	from `bigquery-public-data.ethereum_blockchain.traces` where block_number = #{@block_number}"
    end

    def get_block_info 
=begin
    #return a hash which key is something_you_want_to_query and the value is the result query
    #what field you can 
                        字段名称	类型	模式	说明
                        timestamp	TIMESTAMP	REQUIRED	The timestamp for when the block was collated
                        number	INTEGER	REQUIRED	The block number
                        hash	STRING	REQUIRED	Hash of the block
                        parent_hash	STRING	NULLABLE	Hash of the parent block
                        nonce	STRING	REQUIRED	Hash of the generated proof-of-work
                        sha3_uncles	STRING	NULLABLE	SHA3 of the uncles data in the block
                        logs_bloom	STRING	NULLABLE	The bloom filter for the logs of the block
                        transactions_root	STRING	NULLABLE	The root of the transaction trie of the block
                        state_root	STRING	NULLABLE	The root of the final state trie of the block
                        receipts_root	STRING	NULLABLE	The root of the receipts trie of the block
                        miner	STRING	NULLABLE	The address of the beneficiary to whom the mining rewards were given
                        difficulty	NUMERIC	NULLABLE	Integer of the difficulty for this block
                        total_difficulty	NUMERIC	NULLABLE	Integer of the total difficulty of the chain until this block
                        size	INTEGER	NULLABLE	The size of this block in bytes
                        extra_data	STRING	NULLABLE	The extra data field of this block
                        gas_limit	INTEGER	NULLABLE	The maximum gas allowed in this block
                        gas_used	INTEGER	NULLABLE	The total used gas by all transactions in this block
                        transaction_count	INTEGER	NULLABLE	The number of transactions in the block
=end
        @block_info_hash = Hash.new
        _fields = @fields.split(',')
        _result = @bq.query @sql_query_1
        _result.each do |row|
            _fields.each {|field| @block_info_hash.store(field,row[field.to_sym]) }
        end
        @block_info_hash
    end

    def get_block_transactions
        @block_transaction = []
        puts @sql_query_2
        _result = @bq.query @sql_query_2
        _result.each do |row|
            @block_transaction << row[:transaction_hash]
        end
        @block_transaction
    end

    def get_transaction_info transaction_hash
        @transaction_info = Hash.new
        _sql_query = "SELECT from_address, to_address, input FROM `bigquery-public-data.ethereum_blockchain.transactions` WHERE `hash`=\"#{transaction_hash}\""
        _result = @bq.query _sql_query
        _result.each do |row|
            @transaction_info.store("from_address",row[:from_address])
            @transaction_info.store("to_address",row[:to_address])
            @transaction_info.store("input",row[:input])
        end
        @transaction_info
    end

            
end

class Hunter_finder #forest is the place where the hunter and nonmal people in
    def initialize file,total_supply
        @forest = File.open(file)
        @token_supply = total_supply
        @threshold = 80
        @hunters = []
    end

    def find_hunter
        @forest.each_line do |people|
            _address = people.split(":")[0].chop
            _targe_balance = people.split(":")[1].chop.to_f
            _percentage = _targe_balance / @token_supply
            @hunters << _address if _percentage >= @threshold
        end
    end

    def get_hunters
        find_hunter
        @hunters
    end
end


block_finder = Queryeth_block.new 6109876
block_transactions = block_finder.get_block_transactions
transaction_info = block_finder.get_transaction_info block_transactions[10]
puts transaction_info

=begin
#a example of block_query

block_finder = Queryeth_block.new 66666,'nonce,number'
block_info = block_finder.get_block_info
block_info.each_pair do |key,value|
    puts "#{key}:#{value}"
end
=end

=begin
#a example of contract_query

contact_addr = '0x3ac6cb00f5a44712022a51fbace4c7497f56ee31'
contact_finder = Queryeth_contact.new contact_addr
inputs = contact_finder.get_contact_input
inputs.each {|x| puts x}
=end





