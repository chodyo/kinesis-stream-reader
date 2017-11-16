const chai = require('chai');
const expect = require('chai').expect;
const proxyquire = require('proxyquire').noCallThru();

chai.use(require('chai-http'));


////////////////////////////////////////////////////////////////////////////////
//--------------------------------- mocks -------------------------------------
//\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

const testData = [{ 'record': 1 }, { 'record': 2 }, { 'record': 3 }];
const kinesisStub = {
    getRecords: function (streamname, timestamp) {
        return Promise.resolve(testData);
    }
};
const app = proxyquire('../index.js', { 'kinesisReader': kinesisStub });


////////////////////////////////////////////////////////////////////////////////
//--------------------------------- tests -------------------------------------
//\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

describe('API endpoint /records', function () {
    this.timeout(5000); // How long to wait for a response (ms)

    before(function () {

    });

    after(function () {

    });


    // GET - some data.
    it('should return OK with data', function () {
        return chai.request(app)
            .get('/records?streamname=test-stream')
            .then(function (res) {
                expect(res).to.have.status(200);
                expect(res).to.be.json;
                expect(res.body).to.be.an('array');
                expect(res.body.length).to.equal(3);
                const body = JSON.stringify(res.body);
                const expected = JSON.stringify(testData);
                expect(body).to.equal(expected);
            });
    });

    // GET - invalid path
    it('should return Not Found', function () {
        return chai.request(app)
            .get('/INVALID_PATH')
            .then(function (res) {
                throw new Error('Path exists!');
            })
            .catch(function (err) {
                expect(err).to.have.status(404);
            });
    });

    // GET - no streamname
    it('should return Bad Request with explanation', function () {
        return chai.request(app)
            .get('/records')
            .then(function (res) {
                throw new Error('Path not checking for required query params!');
            })
            .catch(function (err) {
                expect(err).to.have.status(400);
                expect(err.response).to.be.json;
                expect(err.response.body).to.be.an('object');
                expect(err.response.body.badRequest).to.be.a('boolean');
                expect(err.response.body.badRequest).to.equal(true);
                expect(err.response.body.missingRequiredParams).to.be.an('array');
                expect(err.response.body.invalidParams).to.be.an('array');
            });
    });

});