# -*- coding: utf-8 -*-
"""
Created on Tue Feb 20 18:05:18 2024

@author: user
"""

import sys
import os
file_path=os.getcwd()
start_order=file_path.find('DataPipeline')
file_path=file_path[:start_order]
file_path=file_path+'DataPipeline\\pipeline\\'
#os.chdir(file_path)
sys.path.insert(1, file_path)
import unittest
import main



class TestDataQuality(unittest.TestCase): 
    # test functions to test data quality
    def test_customer_count(self): 
        customer_count=main.read_db('select count(*) as count from Customers')
        error_message = "Number of customers is wrong"
        return self.assertEqual(customer_count['count'][0], 10, error_message) 
    
    def test_sales_count(self): 
        products_count=main.read_db('select count(*) as  count from Products')
        error_message = "Number of products is wrong"
        return self.assertEqual(products_count['count'][0], 1000, error_message) 
    
    def test_transformed_count(self): 
        transformed_count=main.read_db('select count(*) as count from Data_Transformed')
        error_message = "Number of transformed data is wrong"
        return self.assertEqual(transformed_count['count'][0], 1000, error_message) 
    def test_aggregated_count(self): 
        aggregated_count=main.read_db('select count(*) as count from Data_Aggregation')
        error_message = "Number of aggregations is wrong"
        return self.assertIsNotNone(aggregated_count['count'][0],  error_message) 
    
if __name__ == '__main__': 
    unittest.main()
    

    