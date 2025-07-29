import pickle
import os
import matplotlib.pyplot as plt
import matplotlib

# Use non-interactive backend if running on a server
matplotlib.use("Agg")

# List of countries for which you have pickle files
countries = ["US", "DE", "FR", "JP", "GB", "IT", "BR", "PL", "ES", "TR"]  

# Loop through each country
for country in countries:
    file_name = f"{country}_categories.pkl"  # Replace with 1km or 2km as needed
    if os.path.exists(file_name):
        with open(file_name, "rb") as file:
            category_counts = pickle.load(file)
        
        # Prepare data for top 10 categories
        sorted_data = sorted(category_counts.items(), key=lambda x: x[1], reverse=True)
        top_10_data = sorted_data[:10]
        top_categories, top_counts = zip(*top_10_data)
        
        # Print the top 10 categories for the country
        print(f"\nTop 10 Categories for {country} (2km):")
        print("-" * 40)
        for category, count in top_10_data:
            print(f"{category:<30} {count:<10}")
        
        # Plot the bar chart
        plt.figure(figsize=(15, 10))
        plt.barh(top_categories, top_counts)
        plt.xlabel("Count")
        plt.ylabel("Category")
        plt.title(f"Top 10 Categories for {country} (2km)")
        plt.tight_layout()
        
        # Save the chart to a file
        output_file = f"{country}_category_bar_chart_2km.png"
        plt.savefig(output_file)
        print(f"Saved top 10 bar chart for {country} as {output_file}")
        plt.close()
    else:
        print(f"Pickle file for {country} not found.")
