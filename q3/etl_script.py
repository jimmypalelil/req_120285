"""
Electric Vehicle Data Warehouse ETL Script

This script extracts electric vehicle data from Washington state,
transforms it into a dimensional model, and loads it into a data warehouse structure.

Author: ETL Solution
Date: July 2025
"""

import pandas as pd
import requests
import sqlite3
import hashlib
from datetime import datetime
from typing import Dict
import warnings
warnings.filterwarnings('ignore')

class EVDataETL:
    """
    Electric Vehicle Data ETL Pipeline

    This class handles the complete ETL process for electric vehicle data
    from extraction to loading into a dimensional data warehouse.
    """

    def __init__(self, data_url: str):
        """
        Initialize the ETL pipeline

        Args:
            data_url (str): URL to download the electric vehicle dataset
        """
        self.data_url = data_url
        self.raw_data = None
        self.cleaned_data = None
        self.dimension_tables = {}
        self.fact_table = None

    def extract_data(self) -> pd.DataFrame:
        """
        Extract data from the source URL in chunks

        Returns:
            pd.DataFrame: Concatenated electric vehicle data from all chunks
        """
        print("Extracting data from source in chunks...")

        try:
            # Download the dataset
            response = requests.get(self.data_url)
            response.raise_for_status()

            # Save to temporary file
            temp_file = 'temp_ev_data.csv'
            with open(temp_file, 'wb') as f:
                f.write(response.content)
            
            # Load into pandas DataFrame (**use chunking for processing lagre files)
            self.raw_data = pd.read_csv('temp_ev_data.csv')
            
            print(f"Successfully extracted {len(self.raw_data)} records")
            print(f"Dataset shape: {self.raw_data.shape}")
            print(f"Columns: {list(self.raw_data.columns)}")
            
            return self.raw_data

        except Exception as e:
            print(f"Error extracting data: {e}")
            raise

    def explore_data(self) -> None:
        """
        Explore and analyze the dataset characteristics
        """
        print("\nEXPLORING DATA CHARACTERISTICS")
        print("=" * 50)

        if self.raw_data is None:
            print("No data available. Please extract data first.")
            return

        # Basic information
        print(f"Dataset Info:")
        print(f"- Shape: {self.raw_data.shape}")
        print(f"- Memory usage: {self.raw_data.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

        # Missing values analysis
        print(f"\nMissing Values Analysis:")
        missing_data = self.raw_data.isnull().sum()
        missing_percent = (missing_data / len(self.raw_data)) * 100
        missing_df = pd.DataFrame({
            'Missing Count': missing_data,
            'Missing Percentage': missing_percent
        }).sort_values('Missing Count', ascending=False)
        print(missing_df[missing_df['Missing Count'] > 0])

        # Analyze three key features as required
        self._analyze_model_year()
        self._analyze_electric_range()
        self._analyze_base_msrp()

    def _analyze_model_year(self) -> None:
        """Analyze Model Year characteristics"""
        print(f"\nMODEL YEAR ANALYSIS")
        print("-" * 30)

        model_year = self.raw_data['Model Year'].dropna()

        print(f"Central Tendency:")
        print(f"- Mean: {model_year.mean():.2f}")
        print(f"- Median: {model_year.median():.2f}")
        print(f"- Mode: {model_year.mode().iloc[0] if len(model_year.mode()) > 0 else 'N/A'}")

        print(f"\nDistribution:")
        print(f"- Range: {model_year.min()} - {model_year.max()}")
        print(f"- IQR: {model_year.quantile(0.75) - model_year.quantile(0.25):.2f}")

        print(f"\nDispersion:")
        print(f"- Standard Deviation: {model_year.std():.2f}")
        print(f"- Variance: {model_year.var():.2f}")
        print(f"- Coefficient of Variation: {(model_year.std() / model_year.mean()) * 100:.2f}%")

    def _analyze_electric_range(self) -> None:
        """Analyze Electric Range characteristics"""
        print(f"\nELECTRIC RANGE ANALYSIS")
        print("-" * 30)

        electric_range = self.raw_data['Electric Range'].dropna()
        electric_range = electric_range[electric_range > 0]  # Remove zero values

        if len(electric_range) == 0:
            print("No valid electric range data available")
            return

        print(f"Central Tendency:")
        print(f"- Mean: {electric_range.mean():.2f} miles")
        print(f"- Median: {electric_range.median():.2f} miles")
        print(f"- Mode: {electric_range.mode().iloc[0] if len(electric_range.mode()) > 0 else 'N/A'} miles")

        print(f"\nDistribution:")
        print(f"- Range: {electric_range.min()} - {electric_range.max()} miles")
        print(f"- IQR: {electric_range.quantile(0.75) - electric_range.quantile(0.25):.2f} miles")

        print(f"\nDispersion:")
        print(f"- Standard Deviation: {electric_range.std():.2f}")
        print(f"- Variance: {electric_range.var():.2f}")
        print(f"- Coefficient of Variation: {(electric_range.std() / electric_range.mean()) * 100:.2f}%")

    def _analyze_base_msrp(self) -> None:
        """Analyze Base MSRP characteristics"""
        print(f"\nBASE MSRP ANALYSIS")
        print("-" * 30)

        base_msrp = self.raw_data['Base MSRP'].dropna()
        base_msrp = base_msrp[base_msrp > 0]  # Remove zero values

        if len(base_msrp) == 0:
            print("No valid Base MSRP data available")
            return

        print(f"Central Tendency:")
        print(f"- Mean: ${base_msrp.mean():.2f}")
        print(f"- Median: ${base_msrp.median():.2f}")
        print(f"- Mode: ${base_msrp.mode().iloc[0] if len(base_msrp.mode()) > 0 else 'N/A'}")

        print(f"\nDistribution:")
        print(f"- Range: ${base_msrp.min():.2f} - ${base_msrp.max():.2f}")
        print(f"- IQR: ${base_msrp.quantile(0.75) - base_msrp.quantile(0.25):.2f}")

        print(f"\nDispersion:")
        print(f"- Standard Deviation: ${base_msrp.std():.2f}")
        print(f"- Variance: ${base_msrp.var():.2f}")
        print(f"- Coefficient of Variation: {(base_msrp.std() / base_msrp.mean()) * 100:.2f}%")

    def clean_and_transform(self) -> pd.DataFrame:
        """
        Clean and transform the raw data

        Returns:
            pd.DataFrame: Cleaned and transformed data
        """
        print("\nCLEANING AND TRANSFORMING DATA")
        print("=" * 50)

        if self.raw_data is None:
            raise ValueError("No raw data available. Please extract data first.")

        # Create a copy for transformation
        self.cleaned_data = self.raw_data.copy()

        # Handle missing values consistently
        self._handle_missing_values()

        # Encode categorical variables
        self._encode_categorical_variables()

        # Create additional derived fields
        self._create_derived_fields()

        print(f"Data cleaning completed. Final shape: {self.cleaned_data.shape}")
        return self.cleaned_data

    def _handle_missing_values(self) -> None:
        """Handle missing values in a consistent way"""
        print("Handling missing values...")

        # Strategy for different types of missing data:

        # 1. Categorical variables - fill with 'Unknown'
        categorical_cols = ['County', 'City', 'Make', 'Model', 'Electric Vehicle Type',
                            'Clean Alternative Fuel Vehicle (CAFV) Eligibility', 'Electric Utility']

        for col in categorical_cols:
            if col in self.cleaned_data.columns:
                missing_count = self.cleaned_data[col].isnull().sum()
                if missing_count > 0:
                    self.cleaned_data[col] = self.cleaned_data[col].fillna('Unknown')
                    print(f"  - {col}: Filled {missing_count} missing values with 'Unknown'")

        # 2. Numeric variables - fill with median for skewed distributions, mean for normal
        if 'Electric Range' in self.cleaned_data.columns:
            electric_range_median = self.cleaned_data['Electric Range'].median()
            missing_count = self.cleaned_data['Electric Range'].isnull().sum()
            self.cleaned_data['Electric Range'] = self.cleaned_data['Electric Range'].fillna(electric_range_median)
            print(f"  - Electric Range: Filled {missing_count} missing values with median ({electric_range_median})")

        if 'Base MSRP' in self.cleaned_data.columns:
            # For MSRP, use 0 to indicate unknown/unavailable pricing
            missing_count = self.cleaned_data['Base MSRP'].isnull().sum()
            self.cleaned_data['Base MSRP'] = self.cleaned_data['Base MSRP'].fillna(0)
            print(f"  - Base MSRP: Filled {missing_count} missing values with 0 (unknown pricing)")

        # 3. Geographic data - forward fill or use 'Unknown'
        geo_cols = ['Postal Code', 'Legislative District', '2020 Census Tract']
        for col in geo_cols:
            if col in self.cleaned_data.columns:
                missing_count = self.cleaned_data[col].isnull().sum()
                if missing_count > 0:
                    self.cleaned_data[col] = self.cleaned_data[col].fillna('Unknown')
                    print(f"  - {col}: Filled {missing_count} missing values with 'Unknown'")

    def _encode_categorical_variables(self) -> None:
        """Encode categorical variables to optimize storage"""
        print("Encoding categorical variables...")

        # Encode Electric Vehicle Type (BEV=1, PHEV=2, Unknown=0)
        if 'Electric Vehicle Type' in self.cleaned_data.columns:
            ev_type_mapping = {
                'Battery Electric Vehicle (BEV)': 1,
                'Plug-in Hybrid Electric Vehicle (PHEV)': 2,
                'Unknown': 0
            }
            self.cleaned_data['EV_Type_Code'] = self.cleaned_data['Electric Vehicle Type'].map(ev_type_mapping)
            self.cleaned_data['EV_Type_Code'] = self.cleaned_data['EV_Type_Code'].fillna(0).astype(int)
            print(f"  - Electric Vehicle Type encoded as EV_Type_Code (BEV=1, PHEV=2, Unknown=0)")

        # Encode CAFV Eligibility (Eligible=1, Not Eligible=2, Unknown=0)
        if 'Clean Alternative Fuel Vehicle (CAFV) Eligibility' in self.cleaned_data.columns:
            cafv_mapping = {
                'Clean Alternative Fuel Vehicle Eligible': 1,
                'Not eligible due to low battery range': 2,
                'Eligibility unknown as battery range has not been researched': 3,
                'Unknown': 0
            }
            self.cleaned_data['CAFV_Code'] = self.cleaned_data['Clean Alternative Fuel Vehicle (CAFV) Eligibility'].map(cafv_mapping)
            self.cleaned_data['CAFV_Code'] = self.cleaned_data['CAFV_Code'].fillna(0).astype(int)
            print(f"  - CAFV Eligibility encoded as CAFV_Code (Eligible=1, Low Range=2, Unknown Research=3, Unknown=0)")

    def _create_derived_fields(self) -> None:
        """Create additional derived fields for analysis"""
        print("Creating derived fields...")

        # Create decade grouping for Model Year
        if 'Model Year' in self.cleaned_data.columns:
            self.cleaned_data['Model_Decade'] = (self.cleaned_data['Model Year'] // 10) * 10
            print(f"  - Created Model_Decade field")

        # Create year category
        current_year = datetime.now().year
        if 'Model Year' in self.cleaned_data.columns:
            self.cleaned_data['Year_Category'] = pd.cut(
                self.cleaned_data['Model Year'],
                bins=[0, 2015, 2020, current_year + 1],
                labels=['Pre-2015', '2015-2020', '2021+'],
                include_lowest=True
            )
            print(f"  - Created Year_Category field")

        # Hash VIN for privacy (keep first 3 chars + hash for tracking)
        if 'VIN (1-10)' in self.cleaned_data.columns:
            self.cleaned_data['VIN_Hash'] = self.cleaned_data['VIN (1-10)'].apply(
                lambda x: hashlib.md5(str(x).encode()).hexdigest()[:10] if pd.notna(x) else 'Unknown'
            )
            print(f"  - Created VIN_Hash field for privacy")

    def create_dimensional_model(self) -> Dict[str, pd.DataFrame]:
        """
        Create dimensional model with fact and dimension tables

        Returns:
            Dict[str, pd.DataFrame]: Dictionary containing all dimension tables and fact table
        """
        print("\nCREATING DIMENSIONAL MODEL")
        print("=" * 50)

        if self.cleaned_data is None:
            raise ValueError("No cleaned data available. Please clean data first.")

        # Create dimension tables
        self._create_location_dimension()
        self._create_vehicle_dimension()
        self._create_utility_dimension()
        self._create_time_dimension()

        # Create fact table
        self._create_fact_table()

        print(f"Dimensional model created with {len(self.dimension_tables)} dimension tables and 1 fact table")

        return {**self.dimension_tables, 'fact_vehicle_registration': self.fact_table}

    def _create_location_dimension(self) -> None:
        """Create location dimension table"""
        print("Creating Location Dimension...")

        location_cols = ['County', 'City', 'State', 'Postal Code', 'Legislative District',
                         '2020 Census Tract', 'Vehicle Location']

        # Extract unique locations
        location_data = self.cleaned_data[location_cols].drop_duplicates().reset_index(drop=True)

        # Parse Vehicle Location into latitude and longitude
        if 'Vehicle Location' in location_data.columns:
            location_data['Latitude'] = None
            location_data['Longitude'] = None

            for idx, row in location_data.iterrows():
                if pd.notna(row['Vehicle Location']) and row['Vehicle Location'] != 'Unknown':
                    try:
                        # Parse "POINT (longitude latitude)" format
                        coords = row['Vehicle Location'].replace('POINT (', '').replace(')', '').split()
                        if len(coords) == 2:
                            location_data.loc[idx, 'Longitude'] = float(coords[0])
                            location_data.loc[idx, 'Latitude'] = float(coords[1])
                    except:
                        pass  # Keep as None if parsing fails

        # Create location_id as primary key
        location_data['location_id'] = range(1, len(location_data) + 1)

        # Reorder columns
        dimension_cols = ['location_id', 'County', 'City', 'State', 'Postal Code',
                          'Legislative District', '2020 Census Tract', 'Latitude', 'Longitude']

        self.dimension_tables['dim_location'] = location_data[dimension_cols]
        print(f"  - Created {len(self.dimension_tables['dim_location'])} unique locations")

    def _create_vehicle_dimension(self) -> None:
        """Create vehicle dimension table"""
        print("Creating Vehicle Dimension...")

        vehicle_cols = ['Make', 'Model', 'Electric Vehicle Type',
                        'Clean Alternative Fuel Vehicle (CAFV) Eligibility',
                        'EV_Type_Code', 'CAFV_Code']

        # Extract unique vehicle combinations
        vehicle_data = self.cleaned_data[vehicle_cols].drop_duplicates().reset_index(drop=True)

        # Create vehicle_id as primary key
        vehicle_data['vehicle_id'] = range(1, len(vehicle_data) + 1)

        # Reorder columns
        dimension_cols = ['vehicle_id', 'Make', 'Model', 'Electric Vehicle Type',
                          'Clean Alternative Fuel Vehicle (CAFV) Eligibility',
                          'EV_Type_Code', 'CAFV_Code']

        self.dimension_tables['dim_vehicle'] = vehicle_data[dimension_cols]
        print(f"  - Created {len(self.dimension_tables['dim_vehicle'])} unique vehicle types")

    def _create_utility_dimension(self) -> None:
        """Create utility dimension table"""
        print("Creating Utility Dimension...")

        # Extract unique utilities
        utility_data = self.cleaned_data[['Electric Utility']].drop_duplicates().reset_index(drop=True)

        # Create utility_id as primary key
        utility_data['utility_id'] = range(1, len(utility_data) + 1)

        # Reorder columns
        self.dimension_tables['dim_utility'] = utility_data[['utility_id', 'Electric Utility']]
        print(f"  - Created {len(self.dimension_tables['dim_utility'])} unique utilities")

    def _create_time_dimension(self) -> None:
        """Create time dimension table"""
        print("Creating Time Dimension...")

        # Extract unique model years and derived time fields
        time_cols = ['Model Year', 'Model_Decade', 'Year_Category']
        time_data = self.cleaned_data[time_cols].drop_duplicates().reset_index(drop=True)

        # Create time_id as primary key
        time_data['time_id'] = range(1, len(time_data) + 1)

        # Reorder columns
        dimension_cols = ['time_id', 'Model Year', 'Model_Decade', 'Year_Category']

        self.dimension_tables['dim_time'] = time_data[dimension_cols]
        print(f"  - Created {len(self.dimension_tables['dim_time'])} unique time periods")

    def _create_fact_table(self) -> None:
        """Create fact table by joining with dimension tables"""
        print("Creating Fact Table...")

        # Start with the main dataset
        fact_data = self.cleaned_data.copy()

        # Join with location dimension to get location_id
        fact_data = fact_data.merge(
            self.dimension_tables['dim_location'],
            on=['County', 'City', 'State', 'Postal Code', 'Legislative District', '2020 Census Tract'],
            how='left'
        )

        # Join with vehicle dimension to get vehicle_id
        fact_data = fact_data.merge(
            self.dimension_tables['dim_vehicle'],
            on=['Make', 'Model', 'Electric Vehicle Type',
                'Clean Alternative Fuel Vehicle (CAFV) Eligibility',
                'EV_Type_Code', 'CAFV_Code'],
            how='left'
        )

        # Join with utility dimension to get utility_id
        fact_data = fact_data.merge(
            self.dimension_tables['dim_utility'],
            on=['Electric Utility'],
            how='left'
        )

        # Join with time dimension to get time_id
        fact_data = fact_data.merge(
            self.dimension_tables['dim_time'],
            on=['Model Year', 'Model_Decade', 'Year_Category'],
            how='left'
        )

        # Create fact table with measures and foreign keys
        fact_cols = ['location_id', 'vehicle_id', 'utility_id', 'time_id',
                     'Base MSRP', 'Electric Range', 'DOL Vehicle ID', 'VIN_Hash']

        self.fact_table = fact_data[fact_cols].copy()

        # Create a unique registration_id for each record
        self.fact_table['registration_id'] = range(1, len(self.fact_table) + 1)

        # Reorder columns to put registration_id first
        final_cols = ['registration_id'] + fact_cols
        self.fact_table = self.fact_table[final_cols]

        print(f"  - Created fact table with {len(self.fact_table)} registrations")

    def load_to_warehouse(self, db_path: str = 'ev_data_warehouse.db') -> None:
        """
        Load the dimensional model to a SQLite data warehouse

        Args:
            db_path (str): Path to the SQLite database file
        """
        print(f"\nLOADING TO DATA WAREHOUSE")
        print("=" * 50)

        if not self.dimension_tables or self.fact_table is None:
            raise ValueError("No dimensional model available. Please create dimensional model first.")

        # Create SQLite connection
        conn = sqlite3.connect(db_path)

        try:
            # Load dimension tables
            for table_name, table_data in self.dimension_tables.items():
                table_data.to_sql(table_name, conn, if_exists='replace', index=False)
                print(f"Loaded {table_name}: {len(table_data)} records")

            # Load fact table
            self.fact_table.to_sql('fact_vehicle_registration', conn, if_exists='replace', index=False)
            print(f"Loaded fact_vehicle_registration: {len(self.fact_table)} records")

            # Create indexes for better performance
            self._create_indexes(conn)

            print(f"\nData warehouse successfully created at: {db_path}")

        finally:
            conn.close()

    def _create_indexes(self, conn: sqlite3.Connection) -> None:
        """Create indexes on foreign keys and commonly queried columns"""
        print("Creating database indexes...")

        index_queries = [
            "CREATE INDEX IF NOT EXISTS idx_fact_location ON fact_vehicle_registration(location_id)",
            "CREATE INDEX IF NOT EXISTS idx_fact_vehicle ON fact_vehicle_registration(vehicle_id)",
            "CREATE INDEX IF NOT EXISTS idx_fact_utility ON fact_vehicle_registration(utility_id)",
            "CREATE INDEX IF NOT EXISTS idx_fact_time ON fact_vehicle_registration(time_id)",
            "CREATE INDEX IF NOT EXISTS idx_location_county ON dim_location(County)",
            "CREATE INDEX IF NOT EXISTS idx_vehicle_make ON dim_vehicle(Make)",
            "CREATE INDEX IF NOT EXISTS idx_time_year ON dim_time('Model Year')"
        ]

        for query in index_queries:
            conn.execute(query)

        conn.commit()
        print("  - Database indexes created")

    def export_to_csv(self, output_dir: str = 'data_warehouse_output') -> None:
        """
        Export all tables to CSV files

        Args:
            output_dir (str): Directory to save CSV files
        """
        import os

        print(f"\nEXPORTING TO CSV FILES")
        print("=" * 50)

        # Create output directory
        os.makedirs(output_dir, exist_ok=True)

        # Export dimension tables
        for table_name, table_data in self.dimension_tables.items():
            file_path = os.path.join(output_dir, f"{table_name}.csv")
            table_data.to_csv(file_path, index=False)
            print(f"Exported {table_name}: {file_path}")

        # Export fact table
        if self.fact_table is not None:
            file_path = os.path.join(output_dir, "fact_vehicle_registration.csv")
            self.fact_table.to_csv(file_path, index=False)
            print(f"Exported fact_vehicle_registration: {file_path}")

        print(f"\nAll files exported to: {output_dir}")

    def generate_summary_report(self) -> None:
        """Generate a summary report of the ETL process"""
        print(f"\nETL PROCESS SUMMARY REPORT")
        print("=" * 50)

        if self.raw_data is not None:
            print(f"Source Data:")
            print(f"  - Records extracted: {len(self.raw_data):,}")
            print(f"  - Columns: {len(self.raw_data.columns)}")

        if self.cleaned_data is not None:
            print(f"\nCleaned Data:")
            print(f"  - Records after cleaning: {len(self.cleaned_data):,}")
            print(f"  - Data quality: {((len(self.cleaned_data) / len(self.raw_data)) * 100):.1f}% retention")

        if self.dimension_tables:
            print(f"\nDimensional Model:")
            for table_name, table_data in self.dimension_tables.items():
                print(f"  - {table_name}: {len(table_data):,} records")

        if self.fact_table is not None:
            print(f"  - fact_vehicle_registration: {len(self.fact_table):,} records")

        print(f"\nETL Process completed successfully!")


def main(export_csv: bool = False):
    """
    Main execution function for the ETL pipeline
    
    Args:
        export_csv (bool): Whether to export dimensional tables to CSV files. 
                          Default False - focuses on core requirement of loading to data warehouse.
    """
    print("ELECTRIC VEHICLE DATA WAREHOUSE ETL")
    print("=" * 60)
    print("This script processes Washington State electric vehicle data")
    print("into a dimensional data warehouse structure.")
    print("=" * 60)

    # Data source URL
    data_url = "https://data.wa.gov/api/views/f6w7-q2d2/rows.csv?accessType=DOWNLOAD"
    
    # Initialize ETL pipeline
    etl = EVDataETL(data_url)

    try:
        # Execute ETL Pipeline
        print("\nStarting ETL Pipeline...")

        # 1. Extract
        etl.extract_data()

        # 2. Explore  
        etl.explore_data()

        # 3. Transform
        etl.clean_and_transform()

        # 4. Create Dimensional Model
        etl.create_dimensional_model()

        # 5. Load to Data Warehouse (CORE REQUIREMENT)
        etl.load_to_warehouse()

        # 6. Export to CSV (OPTIONAL - for inspection/analysis)
        if export_csv:
            print("\nExporting dimensional tables to CSV files...")
            etl.export_to_csv()
        else:
            print("\nSkipping CSV export (use export_csv=True to enable)")

        # 7. Generate Summary
        etl.generate_summary_report()

        print("\n" + "="*60)
        print("ETL PIPELINE COMPLETED SUCCESSFULLY")
        print("="*60)
        print("✅ Data successfully loaded into SQLite data warehouse: ev_data_warehouse.db")
        if export_csv:
            print("✅ Dimensional tables exported to: data_warehouse_output/")
        print("✅ Star schema with 4 dimensions + 1 fact table created")

    except Exception as e:
        print(f"\nETL Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()