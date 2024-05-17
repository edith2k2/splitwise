import sqlite3
import heapq

def simplify(balance):
    sum = 0
    for i in range(len(balance)):
        sum += balance[i][0]
    if sum != 0:
        print("Can't simplify because Not zero sum")
        print(f"sum is {sum}")
        return {}
    negative = []
    positive = []
    for i in range(len(balance)):
        if (balance[i][0] < 0):
            negative.append(balance[i])
        elif balance[i][0] > 0 :
            positive.append(balance[i])
    negative = [(-x, name) for x, name in negative]
    
    heapq.heapify(negative)
    heapq.heapify(positive)

    transactions = []
    while len(negative) and len(positive):
        neg_element = heapq.heappop(negative)
        pos_element = heapq.heappop(positive)
        if (neg_element[0] > pos_element[0]):
            transactions.append((pos_element[1], neg_element[1], pos_element[0]))
            heapq.heappush(negative, (neg_element[0] - pos_element[0], neg_element[1]))
        elif (neg_element[0] < pos_element[0]):
            transactions.append((pos_element[1], neg_element[1], neg_element[0]))
            heapq.heappush(positive, (pos_element[0] - neg_element[0], pos_element[1]))
        else:
            transactions.append((pos_element[1], neg_element[1], neg_element[0]))
    return transactions

class ID():
    def __init__(self, list = None):
        if list != None:
            self.ids = set(list)
        else :
            self.ids = set()
    
    def add_id(self, num):
        self.ids.add(num)

    def get_id(self):
        it = 1
        while it in self.ids:
            it += 1
        return it

class Splitwise():
    def __init__(self, db):
        self.conn = sqlite3.connect("splitwise.db")
        self.db = db

        self.initialise_tables()
    def initialise_tables(self):
        cursor = self.conn.cursor()

        self.tablenames = {0:"Members",1:"Groups", 2:"Transactions", 3:"Group_Transactions", 4:"Group_Members"}
        cursor.execute(f"""create table if not exists {self.tablenames[0]} (
                    id integer,
                    name text,
                    primary key(id)
        ) """)

        cursor.execute(f""" create table if not exists {self.tablenames[1]}(
                    id integer,
                    name text,
                    primary key(id)
        )
        """)

        cursor.execute(f""" create table if not exists {self.tablenames[2]} (
                    id integer,
                    amount integer,
                    member_pos_id integer,
                    member_neg_id integer,
                    primary key(id),
                    foreign key(member_pos_id) references Members(id),
                    foreign key(member_neg_id) references Members(id)
        )""")

        cursor.execute(f''' create table if not exists {self.tablenames[3]} (
                    transaction_id integer,
                    group_id integer,
                    primary key (transaction_id, group_id),
                    foreign key (transaction_id) references Transactions(id),
                    foreign key (group_id) references Groups(id)
        )''')
        
        cursor.execute(f""" create table if not exists {self.tablenames[4]} (
                    group_id integer,
                    member_id integer,
                    amount integer,
                    primary key(group_id, member_id),
                    foreign key(member_id) references Members(id),
                    foreign key(group_id) references Groups(id)
        )""")

        cursor.execute(f''' select id from {self.tablenames[0]}''')
        self.ids = []
        self.ids.append(ID())
        for id in cursor.fetchall():
            self.ids[0].add_id(id[0])
        
        cursor.execute(f''' select id from {self.tablenames[1]}''')
        self.ids.append(ID())
        for id in cursor.fetchall():
            self.ids[1].add_id(id[0])

        self.conn.commit()
        cursor.close()
    
    def check(self, data, table):
        cursor = self.conn.cursor()
        if data.isnumeric():
            cursor.execute(f'''select count(*) from {self.tablenames[table]} where id = ?''', (data))
        else:
            cursor.execute(f'''select count(*) from {self.tablenames[table]} where name = ?''', (data))
        result = cursor.fetchone()[0]
        cursor.close()
        if (result == 0):
            return False
        else :
            return True
        
    def check_relation(self, group_id, member_id, transaction_id = -1):
        cursor = self.conn.cursor()
        if member_id == -1:
            cursor.execute(f''' select count(*) from {self.tablenames[3]} where group_id = {group_id} and transaction_id = {transaction_id}''')
        else:
            cursor.execute(f''' select count(*) from {self.tablenames[4]} where group_id = {group_id} and member_id = {member_id}''')
        result = cursor.fetchone()[0]
        cursor.close()
        if result == 0:
            return False
        else :
            return True
    
    def get_id(self, name, type):
        cursor = self.conn.cursor()
        cursor.execute(f''' select id from {self.tablenames[type]} where name = {name}''')
        return cursor.fetchone()[0]

    def check_table(self, tablename):
        for name in self.tablenames.values():
            if tablename.lower() == name.lower():
                return True
        return False
    
    def add_data(self, data, table, name = None):
        cursor = self.conn.cursor()
        if name != None:
            cursor.execute(f"insert into {self.tablenames[table]} values (?, ?)", (data, name))
            return
        if data.isnumeric():
            print(f"Missing details for {self.tablenames[table][:-1]} with id {data}")
            name = input("Provide Name : ")
            cursor.execute(f"insert into {self.tablenames[table]} values (?, ?)", (data, name))
        else :
            new_id = self.ids[table].get_id()
            cursor.execute(f"insert into {self.tablenames[table]} values (?, ?)", (new_id, data))
        self.conn.commit()
        cursor.close()

    def print_table(self, tablename):
        cursor = self.conn.cursor()
        if tablename.isnumeric(): 
            name = self.tablenames[int(tablename)]
            query = f""" select * from {name}"""
            print(f"---* {name} *--")
        else :
            if self.check_table(tablename) == False: 
                print("No such table")
                return
            query = f""" select * from {tablename}"""
            print(f"---* {tablename} *--")
        cursor.execute(query)
        
        for row in cursor.fetchall():
            print(row)
        cursor.close()

    def transaction_one(self, member_plus, member_minus, group, amnt):
        member_plus_id = member_plus if member_plus.isnumeric() else self.get_id(member_plus)
        member_minus_id = member_minus if member_plus.isnumeric() else self.get_id(member_minus)
        group_id = group if group.isnumeric() else self.get_id(group)
        cursor = self.conn.cursor()
        query = ""
        if self.check_relation(group_id, member_plus_id) == False:
            query = f''' insert into {self.tablenames[4]} values ({group_id}, {member_plus_id}, {amnt}) '''
        else :
            query = f''' update {self.tablenames[4]} set amount = amount + {amnt} 
                        where group_id = {group_id} and member_id = {member_plus_id}'''
        cursor.execute(query)

        if self.check_relation(group_id, member_minus_id) == False:
            query = f''' insert into {self.tablenames[4]} values ({group_id}, {member_minus_id}, {-amnt}) '''
        else :
            query = f''' update {self.tablenames[4]} set amount = amount + {-amnt} 
                        where group_id = {group_id} and member_id = {member_minus_id}'''
        cursor.execute(query)
        cursor.close()
        self.conn.commit()

    def simplify_group(self, group_id):
        # delete from transactions and group_transactions
        cursor = self.conn.cursor()
        cursor.execute(f''' select transaction_id from {self.tablenames[3]} where group_id = {group_id}''')
        transaction_ids = [str(i[0]) for i in cursor.fetchall()] 
        print(transaction_ids)
        if len(transaction_ids) != 0 : 
            cursor.execute(f'''delete from {self.tablenames[2]} where id in ({', '.join(transaction_ids)}) ''')
        cursor.execute(f'''delete from {self.tablenames[3]} where group_id = ?''', (group_id,))
        self.print_table('2')
        self.print_table('3')
        # calculate simplified transactions
        cursor.execute(f''' select amount, member_id from {self.tablenames[4]} where group_id = ?''', (group_id,))
        simplifed_transactions = simplify(cursor.fetchall())

        # get ids
        cursor.execute(f''' select id from {self.tablenames[2]}''')
        existing_ids = ID([i[0] for i in cursor.fetchall()])
        new_ids = []
        transactions = []
        for member_to, member_from, amnt in simplifed_transactions:
            new_id = existing_ids.get_id()
            transactions.append((new_id, amnt, member_to, member_from))
            existing_ids.add_id(new_id)
            new_ids.append(new_id)

        # insert into tables
        cursor.executemany(f''' insert into {self.tablenames[2]} values (?, ?, ?, ?)''', transactions)
        cursor.executemany(f''' insert into {self.tablenames[3]} (transaction_id, group_id) values (?, ?)''', [(trans_id, group_id) for trans_id in new_ids])

        self.conn.commit()
        cursor.close()
    
    def __del__(self):
        self.conn.commit()
        self.conn.close()