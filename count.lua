local function one(record)
	return 1
end

local function sum(v1, v2)
	return v1 + v2
end

function count_star(stream)
	return stream : map(one) : reduce(sum)
end
