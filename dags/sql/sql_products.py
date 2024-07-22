class ProductsSQL:
    def query():
        sql_table = "select\
                        products.id product_id,\
                        products.name,\
                        products.stock,\
                        products.user_id creator_id,\
                        products.created_at,\
                        products.updated_at,\
                        products.status,\
                        products.product_type,\
                        case products.product_type\
                            when 1 then 'BOOK'\
                            when 2 then 'COSMETICS'\
                            when 3 then 'ACCESSORIES'\
                            when 4 then 'ELETRONICS'\
                            when 5 then 'TOYS'\
                            when 6 then 'OTHERS'\
                            else products.product_type\
                        end type_product_describe\
                    from products;"
    
        return sql_table
    
    def fields():
        fields_table=['product_id',
                      'name',
                      'stock',
                      'creator_id',
                      'created_at',
                      'updated_at',
                      'status',
                      'product_type',
                      'type_product_describe']
        
        return fields_table

if __name__ == '__main__':
    print(ProductsSQL.query())
    print(ProductsSQL.fields())