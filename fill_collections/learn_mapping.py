num_list = list(range(1,101))

def square(num):
    return num**2

square_num = map(square, num_list)

print(square_num)