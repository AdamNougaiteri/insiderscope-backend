// services/secDataService.ts

export async function fetchRecentBuyTransactions() {
  const BACKEND_URL =
    "https://insiderscope-backend.vercel.app/api/insider-buys";

  try {
    const response = await fetch(BACKEND_URL, {
      method: "GET",
      headers: {
        Accept: "application/json",
      },
    });

    if (!response.ok) {
      console.error("Backend returned non-200:", response.status);
      return [];
    }

    const data = await response.json();

    // Always guarantee an array
    if (!Array.isArray(data)) {
      console.error("Unexpected backend payload:", data);
      return [];
    }

    // TEMP: tag data so UI knows backend is alive
    return data.map((item, index) => ({
      id: index,
      status: "pending_parse",
      ...item,
    }));
  } catch (error) {
    console.error("Fetch failed:", error);
    return [];
  }
}
