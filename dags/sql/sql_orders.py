class OrdersSQL:
    def query():
        sql_table = "select\
                        orders.id order_id,\
                        orders.customer_id,\
                        items_orders.unitary_value/100 unitary_value,\
                        items_orders.subtotal/100 subtotal,\
                        orders.freight/100 freight,\
                        orders.created_at,\
                        orders.updated_at,\
                        orders.status,\
                        products_packages.product_id,\
                        products.user_id creator_id,\
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
                    from orders\
                    inner join items_orders on items_orders.order_id = orders.id\
                    inner join products_packages on products_packages.package_id = items_orders.package_id\
                    inner join products on product.id = products_packages.product_id\
                    group by orders.id;"
    
        return sql_table

    def fields():
        fields_table = ['order_id',
                        'customer_id',
                        'unitary_value',
                        'subtotal',
                        'freight',
                        'created_at',
                        'updated_at',
                        'status',
                        'product_id',
                        'creator_id',
                        'product_type',
                        'type_product_describe']
    
        return fields_table

if __name__ == '__main__':
    print(OrdersSQL.query())
    print(OrdersSQL.fields())