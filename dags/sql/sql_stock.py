class StockSQL:
    def query():
        sql_table = "select\
                        DATE_FORMAT(stock_history.created_at, '%Y-%m-%d') date,\
                        stock_history.product_id,\
                        users.id creator_id,\
                        sum(CASE type WHEN 1 THEN amount ELSE 0 END) positive,\
                        sum(CASE type WHEN 0 THEN amount ELSE 0 END) negative\
                    from products\
                    inner join users on users.id = products.user_id\
                    inner join stock_history on stock_history.product_id = products.id\
                    group by date, stock_history.product_id\
                    order by stock_history.created_at asc;"
    
        return sql_table
    
    def fields():
        fields_table=['date',
                      'product_id',
                      'creator_id',
                      'positive',
                      'negative']
        
        return fields_table

if __name__ == '__main__':
    print(StockSQL.query())
    print(StockSQL.fields())


    