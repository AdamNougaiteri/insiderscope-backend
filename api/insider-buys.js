export default async function handler(req, res) {
  return res.status(200).json({
    FORCE_PROOF: true,
    timestamp: new Date().toISOString(),
    message: "If you see this, the function is executing",
  });
}
