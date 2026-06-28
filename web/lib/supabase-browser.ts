import { createBrowserClient } from "@supabase/ssr";

/**
 * Supabase client for use in the browser (Client Components).
 *
 * The session cookie is scoped to the root domain (.clearpathdata.org) so the
 * authenticated session is shared across every subdomain (app.clearpathdata.org,
 * and any sibling apps).
 */
export function createClient() {
  return createBrowserClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!,
    {
      cookieOptions: {
        domain: ".clearpathdata.org",
        path: "/",
        sameSite: "lax",
        secure: true,
      },
    }
  );
}
