import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.3";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

serve(async (req: Request) => {
  if (req.method === 'OPTIONS') {
    return new Response('ok', { headers: corsHeaders });
  }

  try {
    const supabaseUrl = Deno.env.get('SUPABASE_URL') ?? '';
    const supabaseServiceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? '';

    if (!supabaseUrl || !supabaseServiceKey) {
      console.error('Supabase Configuração Incompleta');
      throw new Error('Supabase Configuração Incompleta');
    }

    const supabase = createClient(supabaseUrl, supabaseServiceKey, {
      auth: {
        autoRefreshToken: false,
        persistSession: false
      }
    });

    const authHeader = req.headers.get('Authorization');
    if (!authHeader) {
      return new Response(JSON.stringify({ error: "Missing Authorization header" }), {
        status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

    const jwt = authHeader.replace('Bearer ', '');
    const { data: { user }, error: authError } = await supabase.auth.getUser(jwt);
    
    if (authError || !user) {
      return new Response(JSON.stringify({ error: "Unauthorized", details: authError }), {
        status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

    const { data: profile } = await supabase.from('profiles').select('role').eq('id', user.id).single();
    if (!profile || profile.role !== 'admin') {
      return new Response(JSON.stringify({ error: "Forbidden: Admins only" }), {
        status: 403, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

    const body = await req.json();
    const { action, payload } = body;

    let result = null;

    if (action === 'create_user') {
      const { email, password, nome, role, status, empresas_acesso, telas_acesso, centros_custos_acesso } = payload;
      
      const { data: authData, error: createError } = await supabase.auth.admin.createUser({
        email,
        password,
        email_confirm: true
      });
      if (createError) throw createError;

      const { error: profileError } = await supabase.from('profiles').upsert({
        id: authData.user.id,
        nome,
        role,
        status,
        empresas_acesso: empresas_acesso || [],
        telas_acesso: telas_acesso || [],
        // Municípios / grupos de CR liberados; vazio = todos
        centros_custos_acesso: centros_custos_acesso || []
      });
      if (profileError) throw profileError;
      
      result = authData.user;
    } 
    else if (action === 'update_user') {
      const { id, email, password, nome, role, status, empresas_acesso, telas_acesso, centros_custos_acesso } = payload;
      
      const updateData: any = {};
      if (email) updateData.email = email;
      if (password) updateData.password = password;
      
      if (Object.keys(updateData).length > 0) {
        const { error: authUpdateError } = await supabase.auth.admin.updateUserById(id, updateData);
        if (authUpdateError) throw authUpdateError;
      }

      const { error: profileError } = await supabase.from('profiles').update({
        nome,
        role,
        status,
        empresas_acesso: empresas_acesso || [],
        telas_acesso: telas_acesso || [],
        // Municípios / grupos de CR liberados; vazio = todos
        centros_custos_acesso: centros_custos_acesso || []
      }).eq('id', id);
      if (profileError) throw profileError;

      result = { success: true };
    }
    else if (action === 'delete_user') {
      const { id } = payload;
      const { error: deleteError } = await supabase.auth.admin.deleteUser(id);
      if (deleteError) throw deleteError;
      result = { success: true };
    }
    else {
      throw new Error(`Unknown action: ${action}`);
    }

    return new Response(JSON.stringify({ success: true, data: result }), {
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });

  } catch (error: any) {
    console.error('Edge Function Error:', error);
    return new Response(JSON.stringify({ success: false, error: error.message }), {
      status: 400,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }
});
