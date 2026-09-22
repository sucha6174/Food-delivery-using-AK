"""
Simulation Fixtures for Food Delivery Order Tracking System.
Contains comprehensive collections of restaurants, customers, and menu items.
Ensures minimum fixture count requirements:
- >= 20 Restaurants
- >= 30 Customer Names
- >= 40 Menu Items
"""

RESTAURANTS = [
    "Bella Napoli Trattoria",
    "Tokyo Ramen & Izakaya",
    "Spice Symphony Indian Kitchen",
    "Golden Dragon Dim Sum",
    "The Smokehouse BBQ Pit",
    "El Mariachi Taqueria",
    "Mediterranean Breeze",
    "Le Petit Croissant Bistro",
    "Wok & Roll Asian Street Food",
    "Burger Craft Artisan Lab",
    "Green Leaf Vegan Bowls",
    "Siam Secret Thai Cuisine",
    "Bavarian Beer & Pretzel Hall",
    "Seoul Street Korean BBQ",
    "Aloha Poke & Acai Bar",
    "Rustic Crust Sourdough Pizza",
    "Mumbai Masala Express",
    "Kyoto Sushi Atelier",
    "The Hellenic Gyro House",
    "Urban Falafel & Hummus",
    "Little Havana Cuban Cafe",
    "Dragonfly Noodle House",
    "Mamma Mia Pasta Fresca",
    "Harbor Seafood Grille",
    "Fire & Stone Woodfired Pizzas",
    "Zatar Lebanese Kitchen"
]

CUSTOMERS = [
    "Alex Mercer",
    "Brianna Patel",
    "Carlos Ramirez",
    "Devon Washington",
    "Emma Richardson",
    "Farhan Al-Mansoor",
    "Grace Nakamura",
    "Hannah Johansson",
    "Ian MacLeod",
    "Jessica Chen",
    "Kavita Sharma",
    "Liam O'Connor",
    "Maya Lin",
    "Noah Campbell",
    "Olivia Kowalski",
    "Priya Venkatesh",
    "Quinn Sullivan",
    "Rohan Gupta",
    "Sophia Rodriguez",
    "Tyler Brooks",
    "Uma Sundaram",
    "Victor Vance",
    "Wendy Zhao",
    "Xavier Hernandez",
    "Yasmine Benali",
    "Zachary Taylor",
    "Ananya Deshmukh",
    "Brandon Mitchell",
    "Chloe Dubois",
    "Daniel Kim",
    "Elena Rossi",
    "Felix Morales",
    "Gaurav Nair",
    "Harper Reed",
    "Isabella Costa",
    "Justin Blake"
]

MENU_ITEMS = [
    "Margherita Pizza DOC",
    "Truffle Prosciutto Flatbread",
    "Four Cheese Quattro Formaggi",
    "Tonkotsu Pork Ramen",
    "Spicy Miso Ramen",
    "Chicken Karaage Bites",
    "Butter Chicken with Naan",
    "Paneer Tikka Masala",
    "Lamb Rogan Josh",
    "Steamed Pork Soup Dumplings",
    "Crispy Peking Duck Bao",
    "Vegetable Spring Rolls",
    "Slow-Smoked Beef Brisket",
    "Pulled Pork Sliders",
    "Macaroni & Smoked Gouda",
    "Al Pastor Street Tacos",
    "Baja Crispy Fish Tacos",
    "Fresh Guacamole & Tortilla Chips",
    "Greek Village Salad with Feta",
    "Chicken Souvlaki Skewers",
    "Tzatziki with Warm Pita",
    "Classic Double Smashed Burger",
    "Truffle Parmesan Hand-cut Fries",
    "Smoked Bacon Jalapeno Burger",
    "Crispy Tofu Teriyaki Bowl",
    "Avocado & Quinoa Power Salad",
    "Ahi Tuna Poke Bowl",
    "Pad Thai with Wild Shrimp",
    "Green Curry with Coconut Rice",
    "Bavarian Grilled Bratwurst",
    "Giant Salted Soft Pretzel",
    "Korean Fried Chicken Wings",
    "Bibimbap with Fried Egg",
    "Salmon Nigiri Platter",
    "Spicy Tuna Crunch Roll",
    "Dragon Roll with Unagi",
    "Falafel Pocket with Tahini",
    "Creamy Hummus with Pine Nuts",
    "Cuban Pressed Sandwich",
    "Handmade Tagliatelle Bolognese",
    "Creamy Wild Mushroom Risotto",
    "New England Clam Chowder",
    "Crispy Calamari with Garlic Aioli",
    "Garlic Butter Glazed Prawns",
    "Artisan Garlic Breadsticks",
    "Warm Chocolate Lava Cake",
    "Tiramisu Tradizionale",
    "Churros with Dulce de Leche",
    "Mango Sticky Rice",
    "Matcha Green Tea Gelato",
    "Sparkling Blood Orange Soda",
    "Cold Brew Iced Coffee",
    "Iced Hibiscus Berry Tea"
]

def get_fixture_counts() -> dict:
    """Returns the counts of all fixture datasets."""
    return {
        "restaurants": len(RESTAURANTS),
        "customers": len(CUSTOMERS),
        "menu_items": len(MENU_ITEMS),
    }

def validate_fixtures() -> bool:
    """Validates that all fixture datasets meet the required minimum thresholds."""
    counts = get_fixture_counts()
    return (
        counts["restaurants"] >= 20 and
        counts["customers"] >= 30 and
        counts["menu_items"] >= 40
    )
