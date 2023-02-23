class Flat:
    def __init__(self, link, reference=None, price=None, title=None, description=None, posted_date=None,
                 year_of_construction=None, area=None,
                 rooms=None, house_type=None, floor=None, city=None, address=None, district=None,
                 neighborhood=None, images=None):
        self.link = link
        self.reference = reference
        self.price = price
        self.title = title
        self.description = description
        self.posted_date = posted_date
        self.year_of_construction = year_of_construction
        self.area = area
        self.rooms = rooms
        self.house_type = house_type
        self.floor = floor
        self.city = city
        self.address = address
        self.district = district
        self.neighborhood = neighborhood
        self.telephone = '-'
        self.image = images
