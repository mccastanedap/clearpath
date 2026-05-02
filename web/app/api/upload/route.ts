import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

export const runtime = "nodejs";

export async function POST(req: Request) {
  try {
    const formData = await req.formData();
    const file = formData.get("file");
    const businessName = formData.get("businessName");

    if (!businessName || typeof businessName !== "string" || !businessName.trim()) {
      return Response.json(
        { ok: false, error: "Missing business name." },
        { status: 400 }
      );
    }

    if (!file || !(file instanceof File)) {
      return Response.json(
        { ok: false, error: "Missing file." },
        { status: 400 }
      );
    }

    if (!file.name.toLowerCase().endsWith(".csv")) {
      return Response.json(
        { ok: false, error: "File must be a .csv." },
        { status: 400 }
      );
    }

    const bucket = process.env.S3_BUCKET_NAME;
    const region = process.env.AWS_REGION;
    const accessKeyId = process.env.AWS_ACCESS_KEY_ID;
    const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;

    if (!bucket || !region || !accessKeyId || !secretAccessKey) {
      return Response.json(
        { ok: false, error: "S3 credentials not configured on the server." },
        { status: 500 }
      );
    }

    const key = `uploads/${businessName.trim()}/${Date.now()}-${file.name}`;
    const body = Buffer.from(await file.arrayBuffer());

    const s3 = new S3Client({
      region,
      credentials: { accessKeyId, secretAccessKey },
    });

    await s3.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: body,
        ContentType: file.type || "text/csv",
      })
    );

    return Response.json({ ok: true, key });
  } catch (err) {
    const message = err instanceof Error ? err.message : "Unknown error";
    return Response.json(
      { ok: false, error: `Upload failed: ${message}` },
      { status: 500 }
    );
  }
}
