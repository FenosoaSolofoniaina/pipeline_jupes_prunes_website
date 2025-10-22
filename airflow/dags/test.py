import os

print(os.path.abspath(
    os.path.join(
        os.getcwd(),
        '..',
        '..'
        )
    )
)