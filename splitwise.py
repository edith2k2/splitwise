
from utility import *

# balance = [(-104, "vamshi"), (125, "bhavan"), (-578, "gurram"), (242, "peta"), (191, "rohit"), (78, "sath"), (48, "venkat"), (-2, "vishal")]
# transactions = simplify(balance)
# print(transactions)

# conn = sqlite3.connect("splitwise.db")
# cursor = conn.cursor()

# cursor.execute(f''' drop table members''')
# cursor.execute(f''' drop table groups''')
# cursor.execute(f''' drop table group_members''')

# conn.commit()
# cursor.close()
# conn.close()


split = Splitwise("splitwise.db")
while 1:
    menu = ''' Menu :\n 1. simple transaction \n 2. Print table \n 3. Menu\n 4. Quit\n 5. simplify '''
    print(menu)
    option = input("Enter option: ")
    if int(option) == 4 or option == "q":
        print("Bye!")
        break
    elif int(option) == 1:
        simple_transaction_menu = ''' format - TO FROM AMOUNT\n TO FROM GROUP AMOUNT\n menu - menu \n quit - q or quit'''
        print(simple_transaction_menu)
        simple_option = input("Enter Transaction: ")
        if simple_option == "menu":
            pass
        elif simple_option == "quit" or simple_option == "q":
            pass
        else:
            inputs = simple_option.split()
            group = "0" if (len(inputs) == 3) else inputs[2]
            amount = int(inputs[2]) if (len(inputs)== 3) else int(inputs[3])
            if split.check(inputs[0], 0) == False:
                split.add_data(inputs[0], 0)
            if (split.check(inputs[1], 0)) == False:
                split.add_data(inputs[1], 0)
            if len(inputs) == 4 and split.check(inputs[2], 1) == False:
                split.add_data(inputs[2], 1)
            if group == "0" and split.check("0", 1) == False:
                split.add_data("0", 1, "Non_Grouped")
            split.transaction_one(inputs[0], inputs[1], group, amount)
            
    elif int(option) == 5:
        split.simplify_group(0)
            
    elif int(option) == 2:
        print_menu = ''' tablename or table id\n quit - q or quit'''
        print(print_menu)
        print_option = input("Enter table name or id: ")

        if print_option == "quit" or print_option == "q":
            pass
        else: 
            split.print_table(print_option)
