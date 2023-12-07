import { createClient } from "@supabase/supabase-js";

const supabaseUrl = "https://ttydafdojardufehywni.supabase.co";
const supabaseKey =
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InR0eWRhZmRvamFyZHVmZWh5d25pIiwicm9sZSI6ImFub24iLCJpYXQiOjE2OTMzNDk2NDksImV4cCI6MjAwODkyNTY0OX0.X2KKTPFzouQyXAQH3VTrL-fyhbdUtlPsLHIYtoACQss";

export const supabase = createClient(supabaseUrl, supabaseKey);

export default supabase;
