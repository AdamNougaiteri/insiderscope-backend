export default async function handler(req, res) {
  try {
    // Hard-safe static response so function never crashes
    return res.status(200).json([
      {
        id: 'test-1',
        companyName: 'Test Company',
        ticker: 'TEST',
        insiderName: 'Test Insider',
        insiderTitle: 'Director',
        shares: 10000,
        pricePerShare: 100,
        totalValue: 1000000,
        transactionDate: new Date().toISOString()
      }
    ]);
  } catch (err) {
    console.error('API crash:', err);
    return res.status(200).json([]);
  }
}
