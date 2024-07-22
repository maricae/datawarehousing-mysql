class CreatorsSQL:
    def query(): 
        sql_table = "select\
                        users.id creator_id,\
                        users.name,\
                        users.email,\
                        users.status,\
                        users.created_at,\
                        users.updated_at\
                    from users\
                    inner join products on products.user_id = users.id\
                    group by user_id;"
    
        return sql_table
    
    def fields():
        fields_table = ['creator_id',
                        'name',
                        'email',
                        'status',
                        'created_at',
                        'updated_at']
    
        return fields_table

if __name__ == '__main__':
    
    print(CreatorsSQL.query())
    print(CreatorsSQL.fields())