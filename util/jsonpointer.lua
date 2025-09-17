-- This file is generated from teal-src/prosody/util/jsonpointer.tl
local function unescape_token(escaped_token)
	local unescaped = escaped_token:gsub("~1", "/"):gsub("~0", "~")
	return unescaped
end

local function resolve_json_pointer(ref, path)
	local ptr_len = #path + 1
	for part, pos in path:gmatch("/([^/]*)()") do
		local token = unescape_token(part)
		if not (type(ref) == "table") then
			return nil
		end
		local idx = next(ref)
		local new_ref

		if type(idx) == "string" then
			new_ref = ref[token]
		elseif math.type(idx) == "integer" then
			local i = tonumber(token)
			if token == "-" then
				i = #(ref) + 1
			end
			new_ref = ref[i + 1]
		else
			return nil, "invalid-table"
		end

		if pos == ptr_len then
			return new_ref
		elseif type(new_ref) == "table" then
			ref = new_ref
		elseif not (type(ref) == "table") then
			return nil, "invalid-path"
		end

	end
	return ref
end

return { resolve = resolve_json_pointer }
