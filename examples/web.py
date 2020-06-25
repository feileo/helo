import quart
import helo


app = quart.Quart(__name__)
app.config["HELO_DATABASE_URL"] = "mysql://user:password@host:port/db"

db = helo.G(app)


class Author(helo.Model):
    id = helo.BigAuto()
    name = helo.VarChar(length=45, null=False)
    email = helo.Email(default='')
    password = helo.VarChar(length=100, null=False)


@app.route('/api/authors')
async def authors():
    assert quart.current_app.db is db

    await Author.create()
    author = Author(name='at7h', email='g@test.com', password='xxxx')
    await author.save()
    author_list = await Author.select().all(False)
    await Author.drop(safe=True)

    return quart.jsonify(author_list)


app.run()
