import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { createClient } from "@/lib/supabase-server";

export const runtime = "nodejs";

export async function POST(req: Request) {
  try {
    // Identify the user from the Supabase session cookie (.clearpathdata.org).
    const supabase = await createClient();
    const {
      data: { user },
      error: authError,
    } = await supabase.auth.getUser();

    if (authError || !user) {
      return Response.json(
        { ok: false, error: "Unauthorized." },
        { status: 401 }
      );
    }

    const formData = await req.formData();
    const file = formData.get("file");

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

    const key = `uploads/${user.id}/${Date.now()}-${file.name}`;
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
