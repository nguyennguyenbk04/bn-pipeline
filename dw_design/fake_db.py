import random
from faker import Faker
from datetime import datetime, timedelta
import mysql.connector

# Set Faker to default locale for global data
fake = Faker()

# Constants
NUM_ORDERS = 100000
NUM_SELLERS = 4000
NUM_PRODUCTS = 6000
NUM_CUSTOMERS = 7000
ORDER_STATUSES = ['Ordered', 'Shipped', 'Delivered', 'Returned', 'Cancelled']
PAYMENT_METHODS = ['Cash', 'Credit Card', 'Debit Card', 'Bank Transfer', 'Digital Wallet']
PRODUCT_CATEGORIES = [
    'Electronics', 'Fashion', 'Home & Living', 'Sports', 'Beauty', 'Toys',
    'Automotive', 'Books', 'Garden & Outdoors', 'Office Supplies'
]
INVENTORY_REGIONS = ['North America', 'Europe', 'Asia', 'South America', 'Africa', 'Australian']

# Database connection
def get_db_connection():
    return mysql.connector.connect(
        host='127.0.0.1',
        user='bnguyen',
        password='.Tldccmcbtldck2',
        database='online_store'
    )

# Clear all existing data
def clear_existing_data():
    conn = get_db_connection()
    cursor = conn.cursor()
    tables = [
        'Payments', 'OrderItems', 'Reasons', 'Orders', 'Reviews',
        'CartItems', 'ShoppingCarts', 'Addresses', 'Inventory',
        'Products', 'ProductCategories', 'Customers', 'Sellers',
        'PaymentMethods', 'OrderStatus'
    ]
    for table in tables:
        cursor.execute(f"DELETE FROM {table};")
        cursor.execute(f"ALTER TABLE {table} AUTO_INCREMENT = 1;")  # Reset auto-increment
    conn.commit()
    cursor.close()
    conn.close()
    print("Cleared all existing data.")

# Generate Sellers
def generate_sellers(num_sellers):
    sellers = []
    for i in range(1, num_sellers + 1):
        sellers.append({
            'SellerID': i,
            'Name': fake.company(),
            'Email': fake.company_email(),
            'PhoneNumber': fake.phone_number()[:20],
            'CreatedAt': fake.date_time_between(start_date='-2y', end_date='now'),
            'UpdatedAt': fake.date_time_between(start_date='-1y', end_date='now')
        })
    return sellers

# Generate Customers
def generate_customers(num_customers):
    customers = []
    for i in range(1, num_customers + 1):
        customers.append({
            'CustomerID': i,
            'Name': fake.name(),
            'Email': fake.email(),
            'PhoneNumber': fake.phone_number()[:20],
            'CreatedAt': fake.date_time_between(start_date='-2y', end_date='now'),
            'UpdatedAt': fake.date_time_between(start_date='-1y', end_date='now')
        })
    return customers

# Generate Shopping Carts (1 cart per customer)
def generate_shopping_carts(customers):
    carts = []
    for customer in customers:
        carts.append({
            'CartID': customer['CustomerID'],  # CartID matches CustomerID
            'CustomerID': customer['CustomerID'],
            'CreatedAt': fake.date_time_between(start_date='-1y', end_date='now'),
            'UpdatedAt': fake.date_time_between(start_date='-1y', end_date='now')
        })
    return carts

# Generate Cart Items
def generate_cart_items(carts, products):
    cart_items = []
    cart_item_id = 1
    for cart in carts:
        num_items = random.randint(1, 5)  # Each cart has 1-5 items
        for _ in range(num_items):
            cart_items.append({
                'CartItemID': cart_item_id,
                'CartID': cart['CartID'],
                'ProductID': random.choice(products)['ProductID'],
                'Quantity': random.randint(1, 3),
                'CreatedAt': cart['CreatedAt'],
                'UpdatedAt': cart['UpdatedAt']
            })
            cart_item_id += 1
    return cart_items

# Generate Product Categories
def generate_product_categories():
    categories = []
    for i, category in enumerate(PRODUCT_CATEGORIES, start=1):
        categories.append({
            'CategoryID': i,
            'CategoryName': category,
            'CategoryDescription': f"Category for {category.lower()} products",
            'CreatedAt': fake.date_time_between(start_date='-2y', end_date='now'),
            'UpdatedAt': fake.date_time_between(start_date='-1y', end_date='now')
        })
    return categories

# Generate Products
def generate_products(num_products, num_sellers):
    # Predefined products for each category
    product_templates = {
        'Electronics': [
            ('Smartphone', 'A high-end smartphone with advanced features.'),
            ('Laptop', 'A powerful laptop for work and gaming.'),
            ('Headphones', 'Noise-cancelling headphones for immersive sound.'),
            ('Smartwatch', 'A smartwatch with health tracking features.'),
            ('Tablet', 'A lightweight tablet for entertainment and productivity.')
        ],
        'Fashion': [
            ('T-Shirt', 'A comfortable cotton t-shirt.'),
            ('Jeans', 'Stylish denim jeans for everyday wear.'),
            ('Sneakers', 'Lightweight sneakers for casual outings.'),
            ('Dress', 'A fashionable dress for special occasions.'),
            ('Jacket', 'A warm jacket for cold weather.')
        ],
        'Home & Living': [
            ('Sofa', 'A cozy sofa for your living room.'),
            ('Dining Table', 'A wooden dining table for family meals.'),
            ('Lamp', 'A decorative lamp for ambient lighting.'),
            ('Curtains', 'Elegant curtains for your windows.'),
            ('Bookshelf', 'A spacious bookshelf for your collection.')
        ],
        'Sports': [
            ('Football', 'A durable football for outdoor games.'),
            ('Tennis Racket', 'A lightweight tennis racket for professionals.'),
            ('Yoga Mat', 'A non-slip yoga mat for workouts.'),
            ('Basketball', 'An official size basketball.'),
            ('Running Shoes', 'Comfortable running shoes for athletes.')
        ],
        'Beauty': [
            ('Lipstick', 'A long-lasting lipstick in vibrant colors.'),
            ('Face Cream', 'A moisturizing face cream for daily use.'),
            ('Perfume', 'A refreshing perfume with floral notes.'),
            ('Shampoo', 'A nourishing shampoo for healthy hair.'),
            ('Nail Polish', 'Glossy nail polish in various shades.')
        ],
        'Toys': [
            ('Action Figure', 'A collectible action figure for kids.'),
            ('Puzzle', 'A challenging puzzle for brain exercise.'),
            ('Toy Car', 'A remote-controlled toy car for fun.'),
            ('Doll', 'A beautiful doll with accessories.'),
            ('Board Game', 'A fun board game for family nights.')
        ],
        'Automotive': [
            ('Car Vacuum', 'A portable vacuum cleaner for cars.'),
            ('GPS Navigator', 'A GPS navigator with real-time updates.'),
            ('Car Cover', 'A waterproof car cover for protection.'),
            ('Dash Cam', 'A high-definition dash camera.'),
            ('Tire Inflator', 'An electric tire inflator for emergencies.')
        ],
        'Books': [
            ('Mystery Novel', 'A thrilling mystery novel.'),
            ('Cookbook', 'A cookbook with delicious recipes.'),
            ('Science Textbook', 'A comprehensive science textbook.'),
            ('Children\'s Book', 'An illustrated book for children.'),
            ('Travel Guide', 'A guidebook for world travelers.')
        ],
        'Garden & Outdoors': [
            ('Garden Hose', 'A flexible garden hose for watering plants.'),
            ('BBQ Grill', 'A portable BBQ grill for outdoor cooking.'),
            ('Tent', 'A waterproof tent for camping.'),
            ('Lawn Mower', 'An electric lawn mower for easy gardening.'),
            ('Patio Set', 'A stylish patio set for your garden.')
        ],
        'Office Supplies': [
            ('Notebook', 'A ruled notebook for notes.'),
            ('Desk Chair', 'An ergonomic desk chair for comfort.'),
            ('Pen Set', 'A set of smooth-writing pens.'),
            ('Monitor Stand', 'A stand to elevate your monitor.'),
            ('Desk Lamp', 'A bright desk lamp for work.')
        ]
    }

    products = []
    category_count = len(PRODUCT_CATEGORIES)
    used_names = set()
    for i in range(1, num_products + 1):
        category_idx = random.randint(0, category_count - 1)
        category = PRODUCT_CATEGORIES[category_idx]
        name, desc = random.choice(product_templates[category])
        while True:
            unique_code = fake.unique.bothify(text='??-###')
            unique_name = f"{name} {unique_code}"
            if unique_name not in used_names:
                used_names.add(unique_name)
                break
        cost = round(random.uniform(5.00, 400.00), 2)
        price = round(cost * random.uniform(1.18, 1.22), 2)  # Price is ~20% higher than cost
        products.append({
            'ProductID': i,
            'Name': unique_name,
            'Description': desc,
            'Price': price,
            'Cost': cost,
            'CategoryID': category_idx + 1,
            'SellerID': random.randint(1, num_sellers),
            'CreatedAt': fake.date_time_between(start_date='-2y', end_date='now'),
            'UpdatedAt': fake.date_time_between(start_date='-1y', end_date='now')
        })
    return products

# Generate Order Status
def generate_order_status():
    return [
        {'StatusID': 1, 'StatusName': 'Ordered', 'StatusDescription': 'Order has been placed and confirmed'},
        {'StatusID': 2, 'StatusName': 'Shipped', 'StatusDescription': 'Order has been shipped to customer'},
        {'StatusID': 3, 'StatusName': 'Delivered', 'StatusDescription': 'Order has been delivered to customer'},
        {'StatusID': 4, 'StatusName': 'Returned', 'StatusDescription': 'Order has been returned by customer'},
        {'StatusID': 5, 'StatusName': 'Cancelled', 'StatusDescription': 'Order has been cancelled'}
    ]

# Generate Orders
def generate_orders(num_orders, customers):
    orders = []
    # Adjust weights: [Ordered, Shipped, Delivered, Returned, Cancelled]
    status_weights = [0.45, 0.3, 0.2, 0.03, 0.02]  # Returned and Cancelled are now much less frequent and not equal
    for i in range(1, num_orders + 1):
        customer = random.choice(customers)
        status = random.choices(ORDER_STATUSES, weights=status_weights, k=1)[0]
        created_at = fake.date_time_between(start_date='-1y', end_date='now')
        orders.append({
            'OrderID': i,
            'OrderNumber': f"ORD-{fake.random_int(min=100000, max=999999)}",
            'TotalAmount': round(random.uniform(10.00, 1000.00), 2),
            'StatusID': ORDER_STATUSES.index(status) + 1,
            'CustomerID': customer['CustomerID'],
            'CreatedAt': created_at,
            'UpdatedAt': fake.date_time_between(start_date=created_at, end_date='now')
        })
    return orders

# Generate Order Items
def generate_order_items(orders, products):
    order_items = []
    order_item_id = 1
    product_dict = {p['ProductID']: p for p in products}  # Create a lookup for products by ProductID
    for order in orders:
        num_items = random.randint(1, 5)  # Each order has 1-5 items
        for _ in range(num_items):
            product = random.choice(products)
            order_items.append({
                'OrderItemID': order_item_id,
                'OrderID': order['OrderID'],
                'ProductID': product['ProductID'],
                'Quantity': random.randint(1, 3),
                'CurrentPrice': product['Price'],  # Use Price from Products table
                'CreatedAt': order['CreatedAt'],
                'UpdatedAt': order['UpdatedAt']
            })
            order_item_id += 1
    return order_items

# Generate Reasons
def generate_reasons(orders):
    reasons = []
    reason_id = 1

    # Predefined reasons for Returned and Cancelled statuses
    returned_reasons = [
        'Damage or Defects', 'Incorrect Item Received', 'Product Mismatch',
        'Size or Fit Issues', 'Poor Quality', 'Functionality Problems',
        'Customer did not receive'
    ]
    cancelled_reasons = [
        'Change of Mind', 'Found a Better Price', 'Out of Stock',
        'Late Delivery', 'High Shipping Cost'
    ]

    for order in orders:
        status = ORDER_STATUSES[order['StatusID'] - 1]
        if status == 'Returned':
            reason_type = 'Return'
            reason_description = random.choice(returned_reasons)
        elif status == 'Cancelled':
            reason_type = 'Cancellation'
            reason_description = random.choice(cancelled_reasons)
        else:
            continue  # Skip orders with other statuses

        reasons.append({
            'ReasonID': reason_id,
            'OrderID': order['OrderID'],
            'ReasonType': reason_type,
            'ReasonDescription': reason_description,
            'CreatedAt': order['CreatedAt'],
            'UpdatedAt': order['UpdatedAt']
        })
        reason_id += 1

    return reasons

# Generate Reviews
def generate_reviews(products, customers):
    reviews = []
    review_id = 1

    # Review templates by category and rating
    review_templates = {
        'Electronics': {
            5: ["Amazing product! Exceeded my expectations.", "Top-notch quality and features.", "Highly recommend for tech lovers."],
            4: ["Very good, works as described.", "Satisfied with the performance.", "Good value for the price."],
            3: ["Average, does the job.", "It's okay, nothing special.", "Decent for the price."],
            2: ["Not as good as I hoped.", "Some issues with performance.", "Could be better."],
            1: ["Very disappointed. Would not buy again.", "Stopped working quickly.", "Poor quality, not recommended."]
        },
        'Fashion': {
            5: ["Fits perfectly and looks great!", "Stylish and comfortable.", "Love the design and material."],
            4: ["Nice quality, would buy again.", "Looks good, fits well.", "Happy with this purchase."],
            3: ["It's okay, fits as expected.", "Average quality.", "Not bad for the price."],
            2: ["Material feels cheap.", "Didn't fit as expected.", "Not very comfortable."],
            1: ["Terrible fit and quality.", "Very disappointed.", "Would not recommend."]
        },
        'Home & Living': {
            5: ["Makes my home so much better!", "Excellent quality and design.", "Highly recommend for any home."],
            4: ["Looks great in my house.", "Good value for the price.", "Happy with this purchase."],
            3: ["It's okay, does the job.", "Average quality.", "Not bad for the price."],
            2: ["Not as sturdy as expected.", "Some issues with assembly.", "Could be better."],
            1: ["Very poor quality.", "Broke after a week.", "Would not recommend."]
        },
        'Sports': {
            5: ["Perfect for my workouts!", "Great quality sports gear.", "Highly recommend for athletes."],
            4: ["Works well for training.", "Good value for sports lovers.", "Happy with this purchase."],
            3: ["It's okay, does the job.", "Average sports equipment.", "Not bad for the price."],
            2: ["Not as durable as expected.", "Some issues during use.", "Could be better."],
            1: ["Very poor quality.", "Broke after a few uses.", "Would not recommend."]
        },
        'Beauty': {
            5: ["My favorite beauty product!", "Excellent results, highly recommend.", "Love how it feels on my skin."],
            4: ["Works well, would buy again.", "Good quality beauty item.", "Happy with the purchase."],
            3: ["It's okay, nothing special.", "Average results.", "Not bad for the price."],
            2: ["Didn't work as expected.", "Some irritation occurred.", "Could be better."],
            1: ["Very disappointed.", "Caused skin issues.", "Would not recommend."]
        },
        'Toys': {
            5: ["Kids love it!", "Great toy, lots of fun.", "Highly recommend for children."],
            4: ["Good quality toy.", "Fun and entertaining.", "Happy with this purchase."],
            3: ["It's okay, keeps kids busy.", "Average toy.", "Not bad for the price."],
            2: ["Broke after a short time.", "Not as fun as expected.", "Could be better."],
            1: ["Very poor quality.", "Not safe for kids.", "Would not recommend."]
        },
        'Automotive': {
            5: ["Works perfectly for my car!", "Great automotive accessory.", "Highly recommend for drivers."],
            4: ["Good value for the price.", "Useful and reliable.", "Happy with this purchase."],
            3: ["It's okay, does the job.", "Average quality.", "Not bad for the price."],
            2: ["Not as effective as expected.", "Some issues during use.", "Could be better."],
            1: ["Very disappointed.", "Stopped working quickly.", "Would not recommend."]
        },
        'Books': {
            5: ["Couldn't put it down!", "Excellent read, highly recommend.", "Loved every page."],
            4: ["Very good book.", "Enjoyed reading it.", "Would recommend to friends."],
            3: ["It's okay, decent story.", "Average book.", "Not bad for the price."],
            2: ["Not as interesting as expected.", "Some parts were boring.", "Could be better."],
            1: ["Did not enjoy it.", "Very boring.", "Would not recommend."]
        },
        'Garden & Outdoors': {
            5: ["Perfect for my garden!", "Excellent quality outdoor product.", "Highly recommend for gardeners."],
            4: ["Works well in my yard.", "Good value for the price.", "Happy with this purchase."],
            3: ["It's okay, does the job.", "Average garden tool.", "Not bad for the price."],
            2: ["Not as sturdy as expected.", "Some issues with use.", "Could be better."],
            1: ["Very poor quality.", "Broke after a week.", "Would not recommend."]
        },
        'Office Supplies': {
            5: ["Makes my work easier!", "Excellent office product.", "Highly recommend for office use."],
            4: ["Good quality, would buy again.", "Works well at my desk.", "Happy with this purchase."],
            3: ["It's okay, does the job.", "Average office supply.", "Not bad for the price."],
            2: ["Not as durable as expected.", "Some issues during use.", "Could be better."],
            1: ["Very poor quality.", "Broke quickly.", "Would not recommend."]
        }
    }

    # Default templates if category not found
    default_templates = {
        5: ["Excellent product!", "Very satisfied.", "Highly recommend."],
        4: ["Good product.", "Works well.", "Would buy again."],
        3: ["Average.", "It's okay.", "Nothing special."],
        2: ["Not great.", "Had some issues.", "Could be better."],
        1: ["Very bad.", "Not recommended.", "Disappointed."]
    }

    for product in products:
        num_reviews = random.randint(1, 10)
        category = PRODUCT_CATEGORIES[product['CategoryID'] - 1]
        for _ in range(num_reviews):
            customer = random.choice(customers)
            rating = random.randint(1, 5)
            templates = review_templates.get(category, default_templates)
            comment = random.choice(templates.get(rating, default_templates[rating]))
            reviews.append({
                'ReviewID': review_id,
                'ProductID': product['ProductID'],
                'CustomerID': customer['CustomerID'],
                'Rating': rating,
                'Comment': comment,
                'CreatedAt': fake.date_time_between(start_date='-1y', end_date='now'),
                'UpdatedAt': fake.date_time_between(start_date='-1y', end_date='now')
            })
            review_id += 1
    return reviews

# Generate Addresses
def generate_addresses(customers):
    addresses = []
    address_id = 1
    for customer in customers:
        num_addresses = random.randint(1, 2)  # Each customer has 1-2 addresses
        for _ in range(num_addresses):
            addresses.append({
                'AddressID': address_id,
                'CustomerID': customer['CustomerID'],
                'AddressLine': fake.street_address(),
                'City': fake.city(),
                'State': fake.state(),
                'ZipCode': fake.zipcode(),
                'Country': fake.country()[:50],  # Truncate to 50 characters
                'IsBillingAddress': random.choice([0, 1]),
                'IsShippingAddress': random.choice([0, 1]),
                'CreatedAt': fake.date_time_between(start_date='-2y', end_date='now'),
                'UpdatedAt': fake.date_time_between(start_date='-1y', end_date='now')
            })
            address_id += 1
    return addresses

# Generate Inventory
def generate_inventory(products):
    inventory = []
    inventory_id = 1
    region_product_percent = {
        'Asia': 1.0,
        'Europe': 0.7,
        'North America': 0.5,
        'South America': 0.3,
        'Australian': 0.2,
        'Africa': 0.1
    }
    for region in INVENTORY_REGIONS:
        percent = region_product_percent.get(region, 0.1)
        num_products_in_region = int(len(products) * percent)
        region_products = random.sample(products, num_products_in_region)
        for product in region_products:
            quantity = random.randint(0, 1000)
            inventory.append({
                'InventoryID': inventory_id,
                'InventoryName': region,
                'ProductID': product['ProductID'],
                'QuantityInStock': quantity,
                'ReorderThreshold': random.randint(10, 50),
                'CreatedAt': fake.date_time_between(start_date='-2y', end_date='now'),
                'UpdatedAt': fake.date_time_between(start_date='-1y', end_date='now')
            })
            inventory_id += 1
    return inventory

# Generate Payment Methods
def generate_payment_methods():
    return [
        {'PaymentMethodID': 1, 'MethodName': 'Cash', 'MethodDescription': 'Payment made in cash'},
        {'PaymentMethodID': 2, 'MethodName': 'Credit Card', 'MethodDescription': 'Payment using credit card'},
        {'PaymentMethodID': 3, 'MethodName': 'Debit Card', 'MethodDescription': 'Payment using debit card'},
        {'PaymentMethodID': 4, 'MethodName': 'Bank Transfer', 'MethodDescription': 'Payment via bank transfer'},
        {'PaymentMethodID': 5, 'MethodName': 'Digital Wallet', 'MethodDescription': 'Payment using electronic wallet'}
    ]

def generate_payments(orders, payment_methods):
    payments = []
    payment_id = 1
    for order in orders:
        status = ORDER_STATUSES[order['StatusID'] - 1]
        if status in ['Cancelled', 'Returned']:
            continue  # No payment for cancelled or returned orders

        payment_method = random.choice(payment_methods)  # Select a random payment method
        if payment_method['MethodName'] == 'Cash' and status != 'Delivered':
            continue  # Cash payments only for Delivered orders
        if payment_method['MethodName'] != 'Cash' and status not in ['Ordered', 'Shipped', 'Delivered']:
            continue  # Other methods only for Ordered, Shipped, Delivered

        payments.append({
            'PaymentID': payment_id,
            'OrderID': order['OrderID'],  # Use existing OrderID
            'PaymentMethodID': payment_method['PaymentMethodID'],  # Use existing PaymentMethodID
            'Amount': order['TotalAmount'],
            'CreatedAt': order['CreatedAt'] + timedelta(minutes=random.randint(1, 60)),
            'UpdatedAt': order['UpdatedAt']
        })
        payment_id += 1
    return payments

# Insert data into the database
def insert_data_to_db(table_name, data):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Dynamically generate SQL INSERT statements based on table and data
    for record in data:
        columns = ', '.join(record.keys())
        placeholders = ', '.join(['%s'] * len(record))
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.execute(sql, tuple(record.values()))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Inserted {len(data)} records into {table_name}.")

# Main Function
def main():
    print("Clearing existing data...")
    clear_existing_data()

    print("Generating fake data for online store globally...")

    # Generate data
    sellers = generate_sellers(NUM_SELLERS)
    customers = generate_customers(NUM_CUSTOMERS)
    shopping_carts = generate_shopping_carts(customers)
    product_categories = generate_product_categories()
    products = generate_products(NUM_PRODUCTS, NUM_SELLERS)  # Generate Products first
    order_status = generate_order_status()  # Generate OrderStatus data
    payment_methods = generate_payment_methods()  # Generate PaymentMethods data
    orders = generate_orders(NUM_ORDERS, customers)
    cart_items = generate_cart_items(shopping_carts, products)  # Generate CartItems after Products
    order_items = generate_order_items(orders, products)
    reasons = generate_reasons(orders)
    reviews = generate_reviews(products, customers)
    addresses = generate_addresses(customers)
    payments = generate_payments(orders, payment_methods)
    inventory = generate_inventory(products)

    # Insert data into the database
    insert_data_to_db('Sellers', sellers)
    insert_data_to_db('Customers', customers)
    insert_data_to_db('ShoppingCarts', shopping_carts)
    insert_data_to_db('ProductCategories', product_categories)
    insert_data_to_db('Products', products)  # Insert Products before CartItems
    insert_data_to_db('OrderStatus', order_status)  # Insert OrderStatus first
    insert_data_to_db('Orders', orders)
    insert_data_to_db('CartItems', cart_items)  # Insert CartItems after Products
    insert_data_to_db('OrderItems', order_items)
    insert_data_to_db('Reasons', reasons)
    insert_data_to_db('Reviews', reviews)
    insert_data_to_db('Addresses', addresses)
    insert_data_to_db('PaymentMethods', payment_methods)  # Insert PaymentMethods first
    insert_data_to_db('Payments', payments)
    insert_data_to_db('Inventory', inventory)

    # Print summary
    print(f"Inserted {len(sellers)} sellers")
    print(f"Inserted {len(customers)} customers")
    print(f"Inserted {len(shopping_carts)} shopping carts")
    print(f"Inserted {len(cart_items)} cart items")
    print(f"Inserted {len(product_categories)} product categories")
    print(f"Inserted {len(products)} products")
    print(f"Inserted {len(order_status)} order statuses")
    print(f"Inserted {len(orders)} orders")
    print(f"Inserted {len(order_items)} order items")
    print(f"Inserted {len(reasons)} reasons")
    print(f"Inserted {len(reviews)} reviews")
    print(f"Inserted {len(addresses)} addresses")
    print(f"Inserted {len(payment_methods)} payment methods")
    print(f"Inserted {len(payments)} payments")
    print(f"Inserted {len(inventory)} inventory records")

if __name__ == "__main__":
    main()