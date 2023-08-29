import { createClient } from "@supabase/supabase-js";

const supabaseUrl = "";
const supabaseKey = "";

export const supabase = createClient(supabaseUrl, supabaseKey);

export default supabase;