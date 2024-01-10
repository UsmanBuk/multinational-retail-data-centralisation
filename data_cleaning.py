import pandas as pd

class DataCleaning:

    def clean_user_data(self, df):
        """
        Cleans user data in the DataFrame.

        :param df: pandas DataFrame containing user data.
        :return: Cleaned pandas DataFrame.
        """
        # Create a copy of the DataFrame to avoid modifying the original data
        cleaned_df = df.copy()

        # Handle NULL values
        # Option 1: Drop rows with any NULL values
        cleaned_df.dropna(inplace=True)
        
        # Option 2: Fill NULL values with a default value or a computed value (mean, median, etc.)
        # cleaned_df.fillna(value=default_value_or_computed_value, inplace=True)

        # Validate date columns - replace with valid dates or remove invalid rows
        date_columns = ['join_date']  # Example column that contains dates
        for col in date_columns:
            cleaned_df[col] = pd.to_datetime(cleaned_df[col], errors='coerce')  # Converts column to datetime, invalid dates become NaT
            cleaned_df.dropna(subset=[col], inplace=True)  # Drop rows where 'col' has NaT values

        # Check for incorrectly typed values and fix or remove them
        # For example, ensuring a 'phone_number' column is of string type
        cleaned_df['phone_number'] = cleaned_df['phone_number'].astype(str)

        # Identify and handle rows with wrong information
        # This step is highly specific to what constitutes 'wrong information' in your context
        # Example: Remove rows where 'first_name' is a non-character string, or 'age' is negative
        # cleaned_df = cleaned_df[cleaned_df['first_name'].str.isalpha()]
        # cleaned_df = cleaned_df[cleaned_df['age'] > 0]

        return cleaned_df

    
