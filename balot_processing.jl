@time begin

using DataFrames
using XLSX

data_original, headers_original = XLSX.readtable("1920.xlsx", "номинанты")
df_original = DataFrame(data_original, Symbol.(headers_original))

data_nominees, headers_nominees = XLSX.readtable("1920.xlsx", "списки")
nominees = DataFrame(data_nominees, Symbol.(headers_nominees))
    
nomination = [
    "director",
    "actor",
    "actress",
    "actor2",
    "actress2",
    "original_screenplay",
    "adapted_screenplay",
    "operator",
    "editing",
    "soundtrack",
    "art_direction",
    "costumes",
    "make_up",
    "effects",
    "stunts",
    "documentation",
    "russian",
    "live_action_short",
    "animated_short",
    "documentary_short",
    "debut",
    "ensemble",
    "using_music",
    "young_actor",
    "young_actress",
    "special_mentions"
]


function delete_non_breaking_spaces(text)
    # Check if the input is a string
    if text isa String
        # Replace non-breaking spaces and strip leading/trailing whitespace
        return replace(text, "\u00A0" => " ") |> strip
    else
        # Convert non-string inputs to string
        return string(text)
    end
end


function postfix(score)
    if score == 1
        return "$score балл"
    elseif score > 4
        return "$score баллов"
    else
        return "$score балла"
    end
end


function write_down_excel(sheet, name, score, mentions, row, column)
    XLSX.setdata!(sheet, XLSX.CellRef(row, column), String(name)) 
    XLSX.setdata!(sheet, XLSX.CellRef(row, column + 1), string(score)) 
    XLSX.setdata!(sheet, XLSX.CellRef(row, column + 2), string(mentions))
end


df_movies = DataFrame()
df_first = DataFrame()
data = DataFrame()
    
df_original = select!(df_original, Not([:"Отметка времени"]))
df_original = mapcols(x -> coalesce.(x, "xxx"), df_original)
df_transpose = permutedims(df_original,1)
df_transpose = select!(df_transpose, Not([:"Ваш ник на Форуме Кинопоиска:"]))
df_movies = df_transpose[1:10, :]
df_other = df_transpose[11:end, :]
        
rename!(nominees, names(nominees) .=> nomination)
movies_dict = Dict()

for (name, values) in pairs(eachcol(df_movies))
    
    for position in 1:10 
        movie_key = string(values[position])
      
        if haskey(movies_dict, movie_key)
            movies_dict[movie_key]["score"] += 11 - position
            push!(movies_dict[movie_key]["mentions"], (name, postfix(11 - position)))
        else
            movies_dict[movie_key] = Dict(
            "score" => 11 - position, "mentions" => [(name, postfix(11 - position))]
            )
        end
    end
end

nominations_dict = Dict()

for nom in 1:length(nomination)
    nominee = nominees[:, nomination[nom]]
    
    if !(nomination[nom] in keys(nominations_dict))
        nominations_dict[nomination[nom]] = Dict()
        
        for a in 1:length(nominee)
            clean_nominee = delete_non_breaking_spaces(nominee[a])
            
            for (name, values) in pairs(eachcol(df_other))
                if occursin(clean_nominee, string(values[nom]))
                    if haskey(nominations_dict[nomination[nom]], clean_nominee)
                        nominations_dict[nomination[nom]][clean_nominee]["score"] += 1
                        push!(nominations_dict[nomination[nom]][clean_nominee]["mentions"], name)
                    else
                        nominations_dict[nomination[nom]][clean_nominee] = Dict("score" => 1, "mentions" => [name])
                    end
                end
            end
        end
    end
end

nominations_dict

using XLSX
using OrderedCollections

function write_down_excel(sheet, name, score, mentions, row, column)
    XLSX.setdata!(sheet, XLSX.CellRef(row, column), String(name)) 
    XLSX.setdata!(sheet, XLSX.CellRef(row, column + 1), string(score)) 
    XLSX.setdata!(sheet, XLSX.CellRef(row, column + 2), string(mentions))
end

XLSX.openxlsx("1920.xlsx", mode="rw") do xf
winners = xf["победители"]
coincidence = xf["совпадения"]
    
main_categories = ["actor", "actress"]
support_categories = ["actor2", "actress2"]
global row = 1
write_down_excel(coincidence, "actor", "points", "mentions", row, 1)
    
for (index, main_nom) in enumerate(main_categories)
    data_main = nominations_dict[main_nom]  # Make sure nominations_dict is defined and accessible
    data_support = nominations_dict[support_categories[index]]

    for (key, values) in pairs(data_main)
        if values["score"]!=0 || data_support[key]["score"]!= 0
            if haskey(data_support, key)
                global row += 1 
                scores = values["score"] + data_support[key]["score"]
                score = "$scores ($(values["score"]) + $(data_support[key]["score"]))"
                mentions = append!(values["mentions"], data_support[key]["mentions"]) 
                write_down_excel(coincidence, key, score, join(mentions, ", "), row, 1)
            end
        end
    end
end

sorted_movies_dict = OrderedDict(sort(collect(movies_dict), by = (x -> x[2]["score"]), rev = true))
delete!(sorted_movies_dict, "xxx")

for (x, (keys, info)) in enumerate(pairs(sorted_movies_dict))
    write_down_excel(
        winners, keys, info["score"], join(info["mentions"], ", "), x+1, 1
    )
end
    
for (col, nom) in enumerate(nomination)
        sorted_data = OrderedDict(
        sort(collect(nominations_dict[nom]), by = (x -> x[2]["score"]), rev = true)
        )
    for (line, (keys, info)) in enumerate(pairs(sorted_data))
        write_down_excel(
            winners, keys, info["score"], join(info["mentions"], ", "), line+1, 4+((col-1)*3)
        )
    end
end

nomination_plus = ["movie"] ∪ nomination

for z in 1:length(nomination_plus)
    write_down_excel(
        winners,
        nomination_plus[z],
        "points_$(nomination_plus[z])", 
        "mentions_by_$(nomination_plus[z])",
        1,
        (z*3)-2
    )
end

end
end
