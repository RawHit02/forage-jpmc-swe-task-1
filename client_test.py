import unittest
from client3 import getDataPoint


class ClientTest(unittest.TestCase):
    def test_getDataPoint_calculatePrice(self):
        quotes = [
            {'top_ask': {'price': 121.2, 'size': 36}, 'timestamp': '2019-02-11 22:06:30.572453',
             'top_bid': {'price': 120.48, 'size': 109}, 'id': '0.109974697771', 'stock': 'ABC'},
            {'top_ask': {'price': 121.68, 'size': 4}, 'timestamp': '2019-02-11 22:06:30.572453',
             'top_bid': {'price': 117.87, 'size': 81}, 'id': '0.109974697771', 'stock': 'DEF'}
        ]

        """ 
    Test if the calculated price is correct by calling getDataPoint with the provided quotes.
    The expected price is (top_ask['price'] + top_bid['price']) / 2.
    """
        expected_price = (quotes[0]['top_ask']['price'] + quotes[0]['top_bid']['price']) / 2
        self.assertEqual(getDataPoint(quotes), expected_price)

    def test_getDataPoint_calculatePriceBidGreaterThanAsk(self):
        quotes = [
            {'top_ask': {'price': 119.2, 'size': 36}, 'timestamp': '2019-02-11 22:06:30.572453',
             'top_bid': {'price': 120.48, 'size': 109}, 'id': '0.109974697771', 'stock': 'ABC'},
            {'top_ask': {'price': 121.68, 'size': 4}, 'timestamp': '2019-02-11 22:06:30.572453',
             'top_bid': {'price': 117.87, 'size': 81}, 'id': '0.109974697771', 'stock': 'DEF'}
        ]

        """ 
    Test if the calculated price is correct when the bid price is greater than the ask price.
    The expected price is (top_ask['price'] + top_bid['price']) / 2.
    """
        expected_price = (quotes[0]['top_ask']['price'] + quotes[0]['top_bid']['price']) / 2
        self.assertEqual(getDataPoint(quotes), expected_price)

    """ 
  Add more unit tests for other scenarios if needed.
  """


if __name__ == '__main__':
    unittest.main()
