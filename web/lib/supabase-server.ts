import { createServerClient } from "@supabase/ssr";
import { cookies } from "next/headers";

/**
 * Supabase client for use on the server (Route Handlers and Server Components).
 *
 * Reads the request cookies via next/headers so the current session is available
 * server-side. Cookies are written back scoped to the root domain
 * (.clearpathdata.org) so the session stays shared across every subdomain.
 *
 * Note: in Server Components the cookie store is read-only, so the setAll handler
 * is wrapped in a try/catch — writes there are a no-op and can be safely ignored
 * when session refresh is handled elsewhere (e.g. middleware or a route handler).
 */
export async function createClient() {
  const cookieStore = await cookies();

  return createServerClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!,
    {
      cookieOptions: {
        domain: ".clearpathdata.org",
        path: "/",
        sameSite: "lax",
        secure: true,
      },
      cookies: {
        getAll() {
          return cookieStore.getAll();
        },
        setAll(cookiesToSet) {
          try {
            cookiesToSet.forEach(({ name, value, options }) => {
              cookieStore.set(name, value, options);
            });
          } catch {
            // Called from a Server Component where cookies are read-only.
            // Safe to ignore when session refresh happens in middleware / route handlers.
          }
        },
      },
    }
  );
}
